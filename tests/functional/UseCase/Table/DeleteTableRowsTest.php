<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\DeleteTableRowsHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Preview\PreviewTableHandler;

class DeleteTableRowsTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private function setData(string $bucketDatabaseName, string $tableName): void
    {
        // FILL DATA
        $insertGroups = [
            //phpcs:ignore
            'columns' => ['id', 'int', 'decimal', 'decimal_varchar', 'float', 'date', 'time', '_timestamp', 'varchar'],
            'rows' => [
                //phpcs:ignore
                "1, 100, 100.23, '100.23', 100.23456, '2022-01-01', '12:00:02', '2022-01-01 12:00:02', 'Variable character 1'",
                // chanched `time` and `varchar`
                //phpcs:ignore
                "2, 100, 100.23, '100.20', 100.23456, '2022-01-01', '12:00:10', '2022-01-01 12:00:10', 'Variable 2'",
                sprintf(
                    "3, 200, 200.23, '200.23', 200.23456, '2022-01-02', '12:00:10', '2022-01-01 12:00:10', '%s'",
                    str_repeat('VeryLongString123456', 5)
                ),
                '4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
        ];
        $this->fillTableWithData(
            $bucketDatabaseName,
            $tableName,
            $insertGroups,
            true
        );
    }

    /**
     * @param DeleteTableRowsCommand|PreviewTableCommand $command
     */
    private function setPath(string $databaseName, Message $command, string $tableName): void
    {
        if ($databaseName) {
            $path = new RepeatedField(GPBType::STRING);
            $path[] = $databaseName;
            $command->setPath($path);
        }

        if ($tableName) {
            $command->setTableName($tableName);
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse, $connection] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testDeleteTableRows(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'int' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'decimal' => [
                    'type' => Teradata::TYPE_DECIMAL,
                    'length' => '10,2',
                    'nullable' => true,
                ],
                'decimal_varchar' => [
                    'type' => Teradata::TYPE_VARCHAR,
                    'length' => '200',
                    'nullable' => true,
                ],
                'float' => [
                    'type' => Teradata::TYPE_FLOAT,
                    'length' => '',
                    'nullable' => true,
                ],
                'date' => [
                    'type' => Teradata::TYPE_DATE,
                    'length' => '',
                    'nullable' => true,
                ],
                'time' => [
                    'type' => Teradata::TYPE_TIME,
                    'length' => '',
                    'nullable' => true,
                ],
                '_timestamp' => [
                    'type' => Teradata::TYPE_TIMESTAMP,
                    'length' => '',
                    'nullable' => true,
                ],
                'varchar' => [
                    'type' => Teradata::TYPE_VARCHAR,
                    'length' => '',
                    'nullable' => true,
                ],
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($bucketDatabaseName, $tableName, $tableStructure);

        // CHECK: changeSince + changeUntil
        $filter = [
            'input' => [
                'changeSince' => '1641038401',
                'changeUntil' => '1641038403',
            ],
            'expectedRows' => ['2', '3', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: simple where filter
        $filter = [
            'input' => [
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::ge,
                        'values' => ['100'],
                    ]),
                ],
            ],
            'expectedRows' => ['4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 3, 1);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: multiple where filters
        $filter = [
            'input' => [
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::gt,
                        'values' => ['100'],
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::lt,
                        'values' => ['210'],
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::eq,
                        'values' => ['99', '100', '199', '200'],
                    ]),
                ],
            ],
            'expectedRows' => ['1', '2', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: where filter with datatype
        $filter = [
            'input' => [
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'decimal_varchar',
                        'operator' => Operator::eq,
                        'values' => ['100.2'],
                        'dataType' => DataType::REAL,
                    ]),
                ],
            ],
            'expectedRows' => ['1', '3', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // DROP TABLE
        $this->dropTable($bucketDatabaseName, $tableName);
    }

    /**
     * @phpcs:ignore
     * @param array{
     *     changeUntil?: string,
     *     changeSince?: string,
     *     whereFilters?: TableWhereFilter[]
     * } $commandInput
     */
    private function deleteRows(
        string $databaseName,
        string $tableName,
        array $commandInput,
        int $expectedDeletedRowsCount,
        int $expectedRowsCount
    ): PreviewTableResponse {
        $handler = new DeleteTableRowsHandler($this->sessionManager);
        $command = new DeleteTableRowsCommand();
        $this->setPath($databaseName, $command, $tableName);

        if (array_key_exists('changeUntil', $commandInput)) {
            $command->setChangeUntil($commandInput['changeUntil']);
        }
        if (array_key_exists('changeSince', $commandInput)) {
            $command->setChangeSince($commandInput['changeSince']);
        }
        if (array_key_exists('whereFilters', $commandInput)) {
            $command->setWhereFilters($commandInput['whereFilters']);
        }
        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(DeleteTableRowsResponse::class, $response);
        $this->assertSame($expectedDeletedRowsCount, $response->getDeletedRowsCount());
        $this->assertSame($expectedRowsCount, $response->getTableRowsCount());

        // preview data
        $handler = new PreviewTableHandler($this->sessionManager);
        $command = new PreviewTableCommand();

        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'id';
        $command->setColumns($columns);
        $this->setPath($databaseName, $command, $tableName);

        $orderBy = new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class);
        $orderBy[] =
            new ExportOrderBy([
                'columnName' => 'id',
                'order' => ExportOrderBy\Order::ASC,
            ]);
        $command->setOrderBy($orderBy);

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(PreviewTableResponse::class, $response);
        return $response;
    }

    /**
     * @param string[] $expectedRows
     */
    private function checkPreviewData(PreviewTableResponse $response, array $expectedRows): void
    {
        // check rows
        $this->assertCount(count($expectedRows), $response->getRows());
        /** @var PreviewTableResponse\Row[] $rows */
        $rows = $response->getRows();
        foreach ($rows as $rowNumber => $row) {
            $expectedId = $expectedRows[$rowNumber];
            /** @var PreviewTableResponse\Row\Column[] $columns */
            $columns = $row->getColumns();
            foreach ($columns as $column) {
                // check column value
                /** @var Value $columnValue */
                $columnValue = $column->getValue();
                $this->assertSame($expectedId, $columnValue->getStringValue());
            }
        }
    }
}
