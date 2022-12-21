<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\AddPKHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableFilterQueryBuilderFactory;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Throwable;

class PrimaryKeyTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private TableFilterQueryBuilderFactory $tableFilterQueryBuilderFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse, $connection] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;

        $this->tableFilterQueryBuilderFactory = new TableFilterQueryBuilderFactory();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testAddDropPK(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col3' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($bucketDatabaseName, $tableName, $tableStructure);
        $this->fillTableWithData($bucketDatabaseName, $tableName, [[1,2,3], [4,5,6], ]);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames(['col2', 'col3']);
        $addPKHa = new AddPKHandler($this->sessionManager);
        $addPKHa(
            $this->projectCredentials,
            $addPKCommand,
            []
        );
        $db = $this->getConnection($this->projectCredentials);

        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col2', 'col3'], $ref->getPrimaryKeysNames());
    }

    /**
     * @param array{columns: array<string, array<string, mixed>>, primaryKeysNames: array<int, string>} $structure
     */
    private function createTable(string $databaseName, string $tableName, array $structure): void
    {
        $createTableHandler = new CreateTableHandler($this->sessionManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;

        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        /** @var array{type: string, length: string, nullable: bool} $columnData */
        foreach ($structure['columns'] as $columnName => $columnData) {
            $columns[] = (new CreateTableCommand\TableColumn())
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
     * @param array{columns: string, rows: array<int, string>}[] $insertGroups
     */
    private function fillTableWithData(string $databaseName, string $tableName, array $insertGroups): void
    {
        try {
            $db = $this->getConnection($this->projectCredentials);

            foreach ($insertGroups as $insertGroup) {
                foreach ($insertGroup['rows'] as $insertRow) {
                    $insertSql = sprintf(
                        "INSERT INTO %s.%s\n(%s) VALUES\n(%s);",
                        TeradataQuote::quoteSingleIdentifier($databaseName),
                        TeradataQuote::quoteSingleIdentifier($tableName),
                        $insertGroup['columns'],
                        $insertRow
                    );
                    $inserted = $db->executeStatement($insertSql);
                    $this->assertEquals(1, $inserted);
                }
            }
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }
    }

    private function dropTable(string $databaseName, string $tableName): void
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

    /**
     * @phpcs:ignore
     * @param array{
     *     columns: array<string>,
     *     changedSince?: string,
     *     changedUntil?: string,
     *     fulltextSearch?: string,
     *     whereFilters?: TableWhereFilter[],
     *     orderBy?: OrderBy[],
     *     limit?: int
     * } $commandInput
     */
    private function previewTable(string $databaseName, string $tableName, array $commandInput): PreviewTableResponse
    {
        $handler = new PreviewTableHandler($this->sessionManager, $this->tableFilterQueryBuilderFactory);

        $command = new PreviewTableCommand();

        if ($databaseName) {
            $path = new RepeatedField(GPBType::STRING);
            $path[] = $databaseName;
            $command->setPath($path);
        }

        if ($tableName) {
            $command->setTableName($tableName);
        }

        $columns = new RepeatedField(GPBType::STRING);
        foreach ($commandInput['columns'] as $column) {
            $columns[] = $column;
        }
        $command->setColumns($columns);

        if (isset($commandInput['changedSince'])) {
            $command->setChangeSince($commandInput['changedSince']);
        }

        if (isset($commandInput['changedUntil'])) {
            $command->setChangeUntil($commandInput['changedUntil']);
        }

        if (isset($commandInput['fulltextSearch'])) {
            $command->setFulltextSearch($commandInput['fulltextSearch']);
        }

        if (isset($commandInput['whereFilters'])) {
            $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
            foreach ($commandInput['whereFilters'] as $whereFilter) {
                $whereFilters[] = $whereFilter;
            }
            $command->setWhereFilters($whereFilters);
        }

        if (isset($commandInput['orderBy'])) {
            $orderBy = new RepeatedField(GPBType::MESSAGE, OrderBy::class);
            foreach ($commandInput['orderBy'] as $orderByOrig) {
                $orderBy[] = $orderByOrig;
            }
            $command->setOrderBy($orderBy);
        }

        if (isset($commandInput['limit'])) {
            $command->setLimit($commandInput['limit']);
        }

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(PreviewTableResponse::class, $response);
        return $response;
    }

    /**
     * @param string[] $expectedColumns
     * @param array<string, array{value: array<string, mixed>, truncated: bool}>[] $expectedRows
     */
    private function checkPreviewData(PreviewTableResponse $response, array $expectedColumns, array $expectedRows): void
    {
        $columns = ProtobufHelper::repeatedStringToArray($response->getColumns());
        $this->assertEqualsArrays($expectedColumns, $columns);

        // check rows
        $this->assertCount(count($expectedRows), $response->getRows());
        /** @var PreviewTableResponse\Row[] $rows */
        $rows = $response->getRows();
        foreach ($rows as $rowNumber => $row) {
            /** @var array<string, array<string, mixed>> $expectedRow */
            $expectedRow = $expectedRows[$rowNumber];

            // check columns
            /** @var PreviewTableResponse\Row\Column[] $columns */
            $columns = $row->getColumns();
            $this->assertCount(count($expectedRow), $columns);

            foreach ($columns as $column) {
                /** @var array{value: array<string, scalar>, truncated: bool} $expectedColumnValue */
                $expectedColumn = $expectedRow[$column->getColumnName()];

                // check column value
                /** @var array<string, scalar> $expectedColumnValue */
                $expectedColumnValue = $expectedColumn['value'];
                /** @var Value $columnValue */
                $columnValue = $column->getValue();
                $columnValueKind = $columnValue->getKind();
                $this->assertSame(key($expectedColumnValue), $columnValueKind);
                // preview returns all data as string
                if ($columnValueKind === 'null_value') {
                    $this->assertTrue($columnValue->hasNullValue());
                    $this->assertSame(current($expectedColumnValue), $columnValue->getNullValue());
                } elseif ($columnValueKind === 'string_value') {
                    $this->assertTrue($columnValue->hasStringValue());
                    $this->assertSame(current($expectedColumnValue), $columnValue->getStringValue());
                } else {
                    $this->fail(sprintf(
                        "Unsupported value kind '%s' in row #%d and column '%s'",
                        $columnValueKind,
                        $rowNumber,
                        $column->getColumnName()
                    ));
                }

                // check column truncated
                $this->assertSame($expectedColumn['truncated'], $column->getIsTruncated());
            }
        }
    }
}
