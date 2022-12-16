<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\DropColumnHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class AddDropColumnTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse, $connection] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
        $connection->close();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testAddColumn(): void
    {
        $db = $this->getConnection($this->projectCredentials);

        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        $tableDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $command = (new AddColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnDefinition(
                (new TableColumnShared())
                    ->setName('newCol')
                    ->setType(Teradata::TYPE_BIGINT)
                    ->setLength('20')
            );
        $handler = new AddColumnHandler($this->sessionManager);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $newConnection = $this->getConnection($this->projectCredentials);

        $tableRef = new TeradataTableReflection($newConnection, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col1', 'col2', 'col3', 'newCol'], $tableRef->getColumnsNames());
        foreach ($tableRef->getColumnsDefinitions() as $colDef) {
            /** @var ColumnInterface $colDef */
            if ($colDef->getColumnName() === 'newCol') {
                $this->assertEquals(BaseType::INTEGER, $colDef->getColumnDefinition()->getBasetype());
                break;
            }
        }

        $db->close();
    }
    public function testDropColumn(): void
    {
        $db = $this->getConnection($this->projectCredentials);

        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        $tableDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $command = (new DropColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnName('col2')
            ;
        $handler = new DropColumnHandler($this->sessionManager);

        /** @var ObjectInfoResponse $response */
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $newConnection = $this->getConnection($this->projectCredentials);

        $tableRef = new TeradataTableReflection($newConnection, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col1', 'col3'], $tableRef->getColumnsNames());

        $db->close();
    }
}
