<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\AddColumnHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

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
        $db = $this->getConnection($this->getCredentials());

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
                    ->setType(Teradata::TYPE_VARCHAR)
                    ->setLength(Teradata::DEFAULT_NON_LATIN_CHAR_LENGTH)
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

        $db->close();
    }
}
