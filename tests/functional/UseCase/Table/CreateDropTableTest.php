<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class CreateDropTableTest extends BaseCase
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

    public function testCreateTable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->sessionManager);

        $metaIsLatinEnabled = new Any();
        $metaIsLatinEnabled->pack(
            (new CreateTableCommand\TableColumn\TeradataTableColumnMeta())->setIsLatin(true)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
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
            ->setType(Teradata::TYPE_CLOB)
            ->setLength('2097087999') // the biggest length for latin chars, bigger than for unicode chars
            ->setMeta($metaIsLatinEnabled);
        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        $primaryKeysNames[] = 'id';
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($response);

        // CHECK TABLE
        $db = $this->getConnection($this->projectCredentials);

        $this->assertTrue($this->isTableExists($db, $bucketDatabaseName, $tableName));

        $table = new TeradataTableReflection($db, $bucketDatabaseName, $tableName);
        $this->assertEqualsArrays(['id'], $table->getPrimaryKeysNames());

        // check columns
        /** @var TeradataColumn[] $columns */
        $columns = iterator_to_array($table->getColumnsDefinitions());
        $this->assertCount(3, $columns);

        // check column ID
        $column = $columns[0];
        $this->assertSame('id', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Teradata::TYPE_INTEGER, $columnDef->getType());
        $this->assertSame('4', $columnDef->getLength()); // default size
        $this->assertFalse($columnDef->isNullable());
        $this->assertNull($columnDef->getDefault());

        // check column NAME
        $column = $columns[1];
        $this->assertSame('name', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Teradata::TYPE_VARCHAR, $columnDef->getType());
        $this->assertSame('50', $columnDef->getLength());
        $this->assertTrue($columnDef->isNullable());
        $this->assertSame("'Some Default'", $columnDef->getDefault());

        // check column LARGE
        $column = $columns[2];
        $this->assertSame('large', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Teradata::TYPE_CLOB, $columnDef->getType());
        $this->assertSame('2097087999', $columnDef->getLength());
        $this->assertFalse($columnDef->isNullable());
        $this->assertNull($columnDef->getDefault());

        $db->close();

        // DROP TABLE
        $handler = new DropTableHandler($this->sessionManager);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $db = $this->getConnection($this->getCredentials());

        $this->assertFalse($this->isTableExists($db, $bucketDatabaseName, $tableName));

        $db->close();
    }
}
