<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\DbUtils;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;

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
            (new TableColumnShared\TeradataTableColumnMeta())->setIsLatin(true)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
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

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        // CHECK TABLE
        // check columns
        $columns = $response->getTableInfo()->getColumns();
        $this->assertCount(3, $columns);

        // check column ID
        /** @var TableInfo\TableColumn $column */
        $column = $columns[0];
        $this->assertSame('id', $column->getName());
        $this->assertSame(Teradata::TYPE_INTEGER, $column->getType());
        $this->assertSame('4', $column->getLength()); // default size
        $this->assertFalse($column->getNullable());
        $this->assertSame('', $column->getDefault());

        // check column NAME
        /** @var TableInfo\TableColumn $column */
        $column = $columns[1];
        $this->assertSame('name', $column->getName());
        $this->assertSame(Teradata::TYPE_VARCHAR, $column->getType());
        $this->assertSame('50', $column->getLength());
        $this->assertTrue($column->getNullable());
        $this->assertSame("'Some Default'", $column->getDefault());

        // check column LARGE
        /** @var TableInfo\TableColumn $column */
        $column = $columns[2];
        $this->assertSame('large', $column->getName());
        $this->assertSame(Teradata::TYPE_CLOB, $column->getType());
        $this->assertSame('2097087999', $column->getLength());
        $this->assertFalse($column->getNullable());
        $this->assertSame('', $column->getDefault());

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

        $this->assertFalse(DbUtils::isTableExists($db, $bucketDatabaseName, $tableName));

        $db->close();
    }
}
