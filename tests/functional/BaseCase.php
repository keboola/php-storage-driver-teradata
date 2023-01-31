<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Aws\S3\S3Client;
use Doctrine\DBAL\Connection;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableColumnShared\TeradataTableColumnMeta;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\StorageHelper\StorageType;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;
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

//    use RetryTrait;

    /**
     * Set all connections to db here so they can be closed in teardown
     *
     * @var Connection[]
     */
    protected array $dbs = [];

    protected TeradataSessionManager $sessionManager;

    protected GenericBackendCredentials $projectCredentials;

    // to distinguish projects if you need more projects in one test case
    protected string $projectSuffix = '';

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
        return md5(getenv('BUILD_PREFIX') . get_class($this));
    }

    protected function getProjectId(): string
    {
        return md5($this->getName() . $this->getStackPrefix() . $this->projectSuffix);
    }

    protected function getConnection(GenericBackendCredentials $credentials): Connection
    {
        return $this->sessionManager->createSession($credentials);
    }

    /**
     * @return array{0: string, 1: string, 2: string, 3: int, 4: string}
     */
    private function getConnectionParams(): array
    {
        $prefix = (string) getenv('BUILD_PREFIX');
        $storage = (string) getenv('STORAGE_TYPE');

        if (strpos($prefix, 'gh') === 0 && $storage === StorageType::STORAGE_ABS) {
            return [
                (string) getenv('ABS_TERADATA_HOST'),
                (string) getenv('ABS_TERADATA_USERNAME'),
                (string) getenv('ABS_TERADATA_PASSWORD'),
                (int) getenv('ABS_TERADATA_PORT'),
                (string) getenv('ABS_TERADATA_ROOT_DATABASE'),
            ];
        }

        return [
            (string) getenv('TERADATA_HOST'),
            (string) getenv('TERADATA_USERNAME'),
            (string) getenv('TERADATA_PASSWORD'),
            (int) getenv('TERADATA_PORT'),
            (string) getenv('TERADATA_ROOT_DATABASE'),
        ];
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(): GenericBackendCredentials
    {
        [$host, $username, $password, $port, $dbname] = $this->getConnectionParams();

        $any = new Any();
        $any->pack((new GenericBackendCredentials\TeradataCredentialsMeta())->setDatabase(
            $this->getRootDatabase()
        ));

        return (new GenericBackendCredentials())
            ->setHost($host)
            ->setPrincipal($username)
            ->setSecret($password)
            ->setPort($port)
            ->setMeta($any);
    }

    protected function getRootDatabase(): string
    {
        [$host, $username, $password, $port, $dbname] = $this->getConnectionParams();

        return $dbname;
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
        $data = $connection->fetchAllAssociative(
            sprintf(
                "SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = %s AND t.DBKind = 'U'",
                TeradataQuote::quote($name)
            )
        );

        return count($data) > 0;
    }

    /**
     * Check if database exists
     */
    protected function isDatabaseExists(Connection $connection, string $name): bool
    {
        $data = $connection->fetchAllAssociative(
            sprintf(
                "SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = %s AND t.DBKind = 'D'",
                TeradataQuote::quote($name)
            )
        );

        return count($data) > 0;
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
        $meta->pack(
            (new CreateProjectCommand\CreateProjectTeradataMeta())->setRootDatabase(
                $this->getRootDatabase()
            )
            ->setPermSpace('500e6')
            ->setSpoolSpace('500e6')
        );
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

    protected function getBucketId(): string
    {
        return md5($this->getName() . $this->getStackPrefix()) . '_Test_bucket';
    }

    /**
     * @return array{CreateBucketResponse, Connection}
     */
    protected function createTestBucket(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse
    ): array {
        $handler = new CreateBucketHandler($this->sessionManager);
        $meta = new Any();
        $meta->pack(
            (new CreateBucketCommand\CreateBucketTeradataMeta())
                ->setPermSpace('100e6')
                ->setSpoolSpace('100e6')
        );
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($this->getBucketId())
            ->setProjectRoleName($projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName())
            ->setMeta($meta);

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
        return md5($this->getName() . $this->getStackPrefix()) . '_Test_workspace';
    }

    /**
     * @return array{GenericBackendCredentials, CreateWorkspaceResponse}
     */
    protected function createTestWorkspace(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse
    ): array {
        $handler = new CreateWorkspaceHandler($this->sessionManager);
        $meta = new Any();
        $meta->pack(
            (new CreateWorkspaceCommand\CreateWorkspaceTeradataMeta())
                ->setPermSpace('50e6')
                ->setSpoolSpace('50e6')
        );
        $command = (new CreateWorkspaceCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setWorkspaceId($this->getWorkspaceId())
            ->setProjectUserName($projectResponse->getProjectUserName())
            ->setProjectRoleName($projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName())
            ->setMeta($meta);

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
            (new TeradataTableColumnMeta())->setIsLatin(true)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $database;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('id')
            ->setType(Teradata::TYPE_INTEGER);
        $columns[] = (new TableColumnShared())
            ->setName('name')
            ->setType(Teradata::TYPE_VARCHAR)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new TableColumnShared())
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

    /**
     * @param array{columns: array<string, array<string, mixed>>, primaryKeysNames: array<int, string>} $structure
     */
    protected function createTable(string $databaseName, string $tableName, array $structure): void
    {
        $createTableHandler = new CreateTableHandler($this->sessionManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;

        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        /** @var array{type: string, length: string, nullable: bool} $columnData */
        foreach ($structure['columns'] as $columnName => $columnData) {
            $columns[] = (new TableColumnShared())
                ->setName($columnName)
                ->setType($columnData['type'])
                ->setLength($columnData['length'])
                ->setNullable($columnData['nullable']);
        }

        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        foreach ($structure['primaryKeysNames'] as $primaryKeyName) {
            $primaryKeysNames[] = $primaryKeyName;
        }

        $createTableCommand = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $createTableResponse = $createTableHandler(
            $this->projectCredentials,
            $createTableCommand,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $createTableResponse);
        $this->assertSame(ObjectType::TABLE, $createTableResponse->getObjectType());
    }

    /**
     * @param array{columns: string[], rows: string[]} $insertGroups
     */
    protected function fillTableWithData(
        string $databaseName,
        string $tableName,
        array $insertGroups,
        bool $truncate = false
    ): void {
        try {
            $db = $this->getConnection($this->projectCredentials);

            if ($truncate) {
                $db->executeStatement(sprintf(
                    'DELETE %s.%s ALL',
                    TeradataQuote::quoteSingleIdentifier($databaseName),
                    TeradataQuote::quoteSingleIdentifier($tableName),
                ));
            }

            foreach ($insertGroups['rows'] as $insertRow) {
                $insertSql = sprintf(
                    "INSERT INTO %s.%s\n(%s) VALUES\n(%s);",
                    TeradataQuote::quoteSingleIdentifier($databaseName),
                    TeradataQuote::quoteSingleIdentifier($tableName),
                    implode(
                        ',',
                        array_map(fn($item) => TeradataQuote::quoteSingleIdentifier($item), $insertGroups['columns'])
                    ),
                    $insertRow
                );
                $inserted = $db->executeStatement($insertSql);
                $this->assertEquals(1, $inserted);
            }
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }
    }

    protected function dropTable(string $databaseName, string $tableName): void
    {
        $handler = new DropTableHandler($this->sessionManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );
    }
}
