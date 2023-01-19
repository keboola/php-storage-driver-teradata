<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Info;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DBALException;
use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Info\DatabaseInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\SchemaInfo;
use Keboola\StorageDriver\Command\Info\ViewInfo;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\UnknownObjectException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

final class ObjectInfoHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ObjectInfoCommand);

        $db = $this->manager->createSession($credentials);

        $path = ProtobufHelper::repeatedStringToArray($command->getPath());

        assert(count($path) !== 0, 'Error empty path.');

        $response = (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType($command->getExpectedObjectType());

        switch ($command->getExpectedObjectType()) {
            case ObjectType::DATABASE:
                return $this->getDatabaseResponse($path, $db, $response);
            case ObjectType::SCHEMA:
                return $this->getSchemaResponse($path, $db, $response);
            case ObjectType::VIEW:
                return $this->getViewResponse($path, $response);
            case ObjectType::TABLE:
                return $this->getTableResponse($path, $response, $db);
            default:
                throw new UnknownObjectException(ObjectType::name($command->getExpectedObjectType()));
        }
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getChildDBs(Connection $db, string $databaseName): Generator
    {
        /** @var array{child:string} $childDBs */
        $childDBs = $db->fetchFirstColumn(sprintf(
            'SELECT Child AS child FROM DBC.ChildrenVX WHERE Parent = %s',
            TeradataQuote::quote($databaseName)
        ));
        foreach ($childDBs as $child) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::DATABASE)
                ->setObjectName($child);
        }
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getChildObjectsForDB(Connection $db, string $databaseName): Generator
    {
        $ref = new TeradataSchemaReflection($db, $databaseName);
        $tables = $ref->getTablesNames();
        foreach ($tables as $table) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::TABLE)
                ->setObjectName($table);
        }
        $views = $ref->getViewsNames();
        foreach ($views as $view) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::VIEW)
                ->setObjectName($view);
        }
        yield from $this->getChildDBs($db, $databaseName);
    }

    /**
     * @param string[] $path
     */
    private function getDatabaseResponse(array $path, Connection $db, ObjectInfoResponse $response): ObjectInfoResponse
    {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($this->getChildObjectsForDB($db, $path[0]) as $object) {
            $objects[] = $object;
        }
        $this->assertDatabaseExists($objects, $db, $path[0]);

        $infoObject = new DatabaseInfo();
        $infoObject->setObjects($objects);

        $response->setDatabaseInfo($infoObject);

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getSchemaResponse(array $path, Connection $db, ObjectInfoResponse $response): ObjectInfoResponse
    {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $infoObject = new SchemaInfo();
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($this->getChildObjectsForDB($db, $path[0]) as $object) {
            $objects[] = $object;
        }
        $this->assertDatabaseExists($objects, $db, $path[0]);
        $infoObject->setObjects($objects);
        $response->setSchemaInfo($infoObject);
        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getTableResponse(array $path, ObjectInfoResponse $response, Connection $db): ObjectInfoResponse
    {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        try {
            $response->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $path[0],
                new TeradataTableReflection(
                    $db,
                    $path[0],
                    $path[1]
                )
            ));
        } catch (DBALException $e) {
            if (strpos($e->getMessage(), 'does not exist.') !== false) {
                throw new ObjectNotFoundException($path[1]);
            }
            throw $e;
        }
        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getViewResponse(array $path, ObjectInfoResponse $response): ObjectInfoResponse
    {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        $infoObject = new ViewInfo();
        // todo: set view props
        $response->setViewInfo($infoObject);
        return $response;
    }

    private function assertDatabaseExists(RepeatedField $objects, Connection $db, string $databaseName): void
    {
        if ($objects->count() === 0) {
            // test if database exists
            $res = $db->fetchOne(sprintf(
                'SELECT DatabaseName FROM DBC.DatabasesVX WHERE DatabaseName = %s;',
                TeradataQuote::quote($databaseName)
            ));
            if ($res === false) {
                throw new ObjectNotFoundException($databaseName);
            }
        }
    }
}
