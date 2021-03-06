<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Aws\S3\S3Client;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\ExportTableToFileTest;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Teradata\TeradataAccessRight;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use PHPUnit\Framework\TestCase;
use PHPUnitRetry\RetryTrait;

class BaseCase extends TestCase
{
    protected const PROJECT_USER_SUFFIX = '_KBC_user';
    protected const PROJECT_ROLE_SUFFIX = '_KBC_role';
    protected const PROJECT_READ_ONLY_ROLE_SUFFIX = '_KBC_RO';
    protected const PROJECT_PASSWORD = 'PassW0rd#';

    use RetryTrait;

    /**
     * Set all connections to db here so they can be closed in teardown
     *
     * @var Connection[]
     */
    protected array $dbs = [];

    protected TeradataSessionManager $sessionManager;

    /**
     * @param array<mixed> $data
     * @param int|string $dataName
     */
    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->sessionManager = new TeradataSessionManager(static::isDebug());
    }

    /**
     * Check if parent has child
     */
    protected function isChildOf(Connection $connection, string $child, string $parent): bool
    {
        $exists = $connection->fetchAllAssociative(sprintf(
            'SELECT * FROM DBC.ChildrenV WHERE Child = %s AND Parent = %s;',
            TeradataQuote::quote($child),
            TeradataQuote::quote($parent)
        ));

        return count($exists) === 1;
    }

    protected function cleanTestProject(): void
    {
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $this->getStackPrefix()
        );
        $projectRoleName = $nameGenerator->createRoleNameForProject($this->getProjectId());
        $projectReadOnlyRoleName = $nameGenerator->createReadOnlyRoleNameForProject($this->getProjectId());
        $projectUsername = $nameGenerator->createUserNameForProject($this->getProjectId());

        $db = $this->getConnection($this->getCredentials());
        $this->cleanUserOrDatabase($db, $projectUsername, $this->getCredentials()->getPrincipal());
        $this->dropRole($db, $projectReadOnlyRoleName);
        $this->dropRole($db, $projectRoleName);
        $db->close();
    }

    protected function cleanTestWorkspace(): void
    {
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $this->getStackPrefix()
        );
        $workspaceUserName = $nameGenerator->createWorkspaceUserNameForWorkspaceId($this->getWorkspaceId());
        $workspaceRoleName = $nameGenerator->createWorkspaceRoleNameForWorkspaceId($this->getWorkspaceId());

        $db = $this->getConnection($this->getCredentials());
        $this->cleanUserOrDatabase($db, $workspaceUserName, $this->getCredentials()->getPrincipal());
        $this->dropRole($db, $workspaceRoleName);
        $db->close();
    }

    protected function getStackPrefix(): string
    {
        return md5(get_class($this));
    }

    protected function getProjectId(): string
    {
        return md5($this->getName());
    }

    protected function getConnection(GenericBackendCredentials $credentials): Connection
    {
        return $this->sessionManager->createSession($credentials);
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(): GenericBackendCredentials
    {
        $any = new Any();
        $any->pack((new GenericBackendCredentials\TeradataCredentialsMeta())->setDatabase(
            $this->getRootDatabase()
        ));

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('TERADATA_HOST'))
            ->setPrincipal((string) getenv('TERADATA_USERNAME'))
            ->setSecret((string) getenv('TERADATA_PASSWORD'))
            ->setPort((int) getenv('TERADATA_PORT'))
            ->setMeta($any);
    }

    protected function getRootDatabase(): string
    {
        return (string) getenv('TERADATA_ROOT_DATABASE');
    }

    /**
     * Drop user and child objects
     *
     * @param string $cleanerUser - Connected user which will be granted access to drop user/database
     */
    protected function cleanUserOrDatabase(Connection $connection, string $name, string $cleanerUser): void
    {
        $isUserExists = $this->isUserExists($connection, $name);
        $isDatabaseExists = $this->isDatabaseExists($connection, $name);

        if (!$isUserExists && !$isDatabaseExists) {
            return;
        }

        $connection->executeStatement(sprintf(
            'GRANT ALL ON %s TO %s',
            TeradataQuote::quoteSingleIdentifier($name),
            $cleanerUser
        ));
        $connection->executeStatement(sprintf(
            'GRANT DROP USER, DROP DATABASE ON %s TO %s',
            TeradataQuote::quoteSingleIdentifier($name),
            $cleanerUser
        ));
        if ($isUserExists) {
            $connection->executeStatement(sprintf('DELETE USER %s ALL', TeradataQuote::quoteSingleIdentifier($name)));
            $childDatabases = $this->getChildDatabases($connection, $name);
            foreach ($childDatabases as $childDatabase) {
                $this->cleanUserOrDatabase($connection, $childDatabase, $cleanerUser);
            }
            $connection->executeStatement(sprintf('DROP USER %s', TeradataQuote::quoteSingleIdentifier($name)));
        } else {
            $connection->executeStatement(sprintf(
                'DELETE DATABASE %s ALL',
                TeradataQuote::quoteSingleIdentifier($name)
            ));
            $childDatabases = $this->getChildDatabases($connection, $name);
            foreach ($childDatabases as $childDatabase) {
                $this->cleanUserOrDatabase($connection, $childDatabase, $cleanerUser);
            }
            $connection->executeStatement(sprintf('DROP DATABASE %s', TeradataQuote::quoteSingleIdentifier($name)));
        }
    }

    /**
     * Check if user exists
     */
    protected function isUserExists(Connection $connection, string $name): bool
    {
        try {
            $connection->executeStatement(sprintf('HELP USER %s', TeradataQuote::quoteSingleIdentifier($name)));
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     * Check if database exists
     */
    protected function isDatabaseExists(Connection $connection, string $name): bool
    {
        try {
            $connection->executeStatement(sprintf('HELP DATABASE %s', TeradataQuote::quoteSingleIdentifier($name)));
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     * @return string[]
     */
    protected function getChildDatabases(Connection $connection, string $name): array
    {
        /** @var string[] $return */
        $return = $connection->fetchFirstColumn(sprintf(
            'SELECT Child FROM DBC.ChildrenV WHERE Parent = %s;',
            TeradataQuote::quote($name),
        ));
        return $return;
    }

    /**
     * Drop role if exists
     */
    protected function dropRole(Connection $connection, string $roleName): void
    {
        if (!$this->isRoleExists($connection, $roleName)) {
            return;
        }

        $connection->executeQuery(sprintf(
            'DROP ROLE %s',
            TeradataQuote::quoteSingleIdentifier($roleName)
        ));
    }

    /**
     * check if role exists
     */
    protected function isRoleExists(Connection $connection, string $roleName): bool
    {
        $roles = $connection->fetchAllAssociative(sprintf(
            'SELECT RoleName FROM DBC.RoleInfoV WHERE RoleName = %s',
            TeradataQuote::quote($roleName)
        ));
        return count($roles) === 1;
    }

    protected function isTableExists(Connection $connection, string $databaseName, string $tableName): bool
    {
        $tables = $connection->fetchAllAssociative(sprintf(
            'SELECT TableName FROM DBC.TablesVX WHERE DatabaseName = %s AND TableName = %s',
            TeradataQuote::quote($databaseName),
            TeradataQuote::quote($tableName)
        ));
        return count($tables) === 1;
    }

    protected function getProjectReadOnlyRole(): string
    {
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $this->getStackPrefix()
        );
        return $nameGenerator->createReadOnlyRoleNameForProject($this->getProjectId());
    }

    protected function getProjectRole(): string
    {
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $this->getStackPrefix()
        );
        return $nameGenerator->createRoleNameForProject($this->getProjectId());
    }

    /**
     * Create test project scoped for each test name
     *
     * @return array{GenericBackendCredentials,CreateProjectResponse}
     */
    protected function createTestProject(): array
    {
        $handler = new CreateProjectHandler($this->sessionManager);
        $meta = new Any();
        $meta->pack((new CreateProjectCommand\CreateProjectTeradataMeta())->setRootDatabase(
            $this->getRootDatabase()
        ));
        $command = (new CreateProjectCommand())
            ->setProjectId($this->getProjectId())
            ->setStackPrefix($this->getStackPrefix())
            ->setMeta($meta);

        $response = $handler(
            $this->getCredentials(),
            $command,
            []
        );
        assert($response instanceof CreateProjectResponse);

        $rootCredentials = $this->getCredentials();
        return [
            (new GenericBackendCredentials())
                ->setHost($rootCredentials->getHost())
                ->setPrincipal($response->getProjectUserName())
                ->setSecret($response->getProjectPassword())
                ->setPort($rootCredentials->getPort()),
            $response,
        ];
    }

    /**
     * Get test project credentials scoped for each test name
     */
    protected function getTestProjectCredentials(): GenericBackendCredentials
    {
        $rootCredentials = $this->getCredentials();
        return (new GenericBackendCredentials())
            ->setHost($rootCredentials->getHost())
            ->setPrincipal($this->getProjectUser())
            ->setSecret(self::PROJECT_PASSWORD)
            ->setPort($rootCredentials->getPort());
    }

    protected function getProjectUser(): string
    {
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $this->getStackPrefix()
        );
        return $nameGenerator->createUserNameForProject($this->getProjectId());
    }

    /**
     * Get list of AccessRight's of user on database
     *
     * @return string[]
     */
    protected function getRoleAccessRightForDatabase(Connection $db, string $role, string $database): array
    {
        /** @var string[] $return */
        $return = $db->fetchFirstColumn(sprintf(
            'SELECT AccessRight FROM DBC.AllRoleRightsV WHERE RoleName = %s AND DatabaseName = %s',
            TeradataQuote::quote($role),
            TeradataQuote::quote($database)
        ));
        return $return;
    }

    /**
     * Get list of AccessRight's of user on database
     *
     * @return string[]
     */
    protected function getUserAccessRightForDatabase(Connection $db, string $user, string $database): array
    {
        /** @var string[] $return */
        $return = $db->fetchFirstColumn(sprintf(
            'SELECT AccessRight FROM DBC.AllRightsV WHERE UserName = %s AND DatabaseName = %s',
            TeradataQuote::quote($user),
            TeradataQuote::quote($database)
        ));
        return $return;
    }

    /**
     * @param array<mixed> $expected
     * @param array<mixed> $actual
     */
    protected function assertEqualsArrays(array $expected, array $actual): void
    {
        sort($expected);
        sort($actual);

        $this->assertEquals($expected, $actual);
    }

    /**
     * @return array{CreateBucketResponse, Connection}
     */
    protected function createTestBucket(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse
    ): array {
        $bucket = md5($this->getName()) . '_Test_bucket';

        $handler = new CreateBucketHandler($this->sessionManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bucket)
            ->setProjectRoleName($projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName());

        $response = $handler(
            $projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);

        $db = $this->getConnection($projectCredentials);

        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // read only role has read access to bucket
        $this->assertEqualsArrays(
            [TeradataAccessRight::RIGHT_RETRIEVE_OR_SELECT],
            $this->getRoleAccessRightForDatabase(
                $db,
                $projectResponse->getProjectReadOnlyRoleName(),
                $response->getCreateBucketObjectName()
            )
        );

        return [$response, $db];
    }

    protected function getWorkspaceId(): string
    {
        return md5($this->getName()) . '_Test_workspace';
    }

    /**
     * @return array{GenericBackendCredentials, CreateWorkspaceResponse}
     */
    protected function createTestWorkspace(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse
    ): array {
        $handler = new CreateWorkspaceHandler($this->sessionManager);
        $command = (new CreateWorkspaceCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setWorkspaceId($this->getWorkspaceId())
            ->setProjectUserName($projectResponse->getProjectUserName())
            ->setProjectRoleName($projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName());

        $response = $handler(
            $projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $credentials = (new GenericBackendCredentials())
            ->setHost($projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($projectCredentials->getPort());
        return [$credentials, $response];
    }

    protected function createTestTable(
        GenericBackendCredentials $credentials,
        string $database,
        ?string $tableName = null
    ): string {
        if ($tableName === null) {
            $tableName = md5($this->getName()) . '_Test_table';
        }

        // CREATE TABLE
        $handler = new CreateTableHandler($this->sessionManager);

        $metaIsLatinEnabled = new Any();
        $metaIsLatinEnabled->pack(
            (new CreateTableCommand\TableColumn\TeradataTableColumnMeta())->setIsLatin(true)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $database;
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('id')
            ->setType(Teradata::TYPE_INTEGER);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('name')
            ->setType(Teradata::TYPE_VARCHAR)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('large')
            ->setType(Teradata::TYPE_VARCHAR)
            ->setLength('10000')
            ->setMeta($metaIsLatinEnabled);
        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        $primaryKeysNames[] = 'id';
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $handler(
            $credentials,
            $command,
            []
        );
        return $tableName;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->sessionManager->close();
    }

    protected static function isDebug(): bool
    {
        return (bool) getenv('DEBUG');
    }

    protected function getS3Client(string $key, string $secret, string $region): S3Client
    {
        return new S3Client([
            'credentials' => [
                'key' => $key,
                'secret' => $secret,
            ],
            'region' => $region,
            'version' => '2006-03-01',
        ]);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    protected function listS3BucketDirFiles(S3Client $client, string $bucket, string $dir): ?array
    {
        $result = $client->listObjects([
            'Bucket' => $bucket,
            'Prefix' => $dir,
        ]);
        /** @var array<int, array<string, mixed>> $contents */
        $contents = $result->get('Contents');
        return $contents;
    }

    protected function clearS3BucketDir(S3Client $client, string $bucket, string $dir): void
    {
        $objects = $this->listS3BucketDirFiles($client, $bucket, $dir);

        if ($objects) {
            $client->deleteObjects([
                'Bucket' => $bucket,
                'Delete' => [
                    'Objects' => array_map(static function ($object) {
                        return [
                            'Key' => $object['Key'],
                        ];
                    }, $objects),
                ],
            ]);
        }
    }
}
