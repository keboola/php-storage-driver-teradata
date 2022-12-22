<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Aws\S3\S3Client;
use Doctrine\DBAL\Connection;
use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Teradata\QueryBuilder\ExportQueryBuilderFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

class ExportTableToFileTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private ExportQueryBuilderFactory $tableExportQueryBuilderFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse,] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;

        $this->tableExportQueryBuilderFactory = new ExportQueryBuilderFactory();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    /**
     * @dataProvider simpleExportProvider
     * @param array{exportOptions: ExportOptions} $input
     * @param int[] $expectedResultFileSize
     * @param array<int, string>[]|null $expectedResultData
     */
    public function testExportTableToFile(
        array $input,
        ?array $expectedResultFileSize,
        ?array $expectedResultData,
        ?int $expectedRowsCount = null
    ): void {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getName())
        );

        // create table
        $db = $this->getConnection($this->projectCredentials);
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $db);

        // clear files
        $s3Client = $this->getS3Client(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION')
        );
        $this->clearS3BucketDir(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $cmd->setFileProvider(FileProvider::S3);

        $cmd->setFileFormat(FileFormat::CSV);

        if ($input['exportOptions'] instanceof ExportOptions) {
            $cmd->setExportOptions($input['exportOptions']);
        }

        $exportMeta = new Any();
        $exportMeta->pack(
            (new TableExportToFileCommand\TeradataTableExportMeta())
                ->setExportAdapter(TableExportToFileCommand\TeradataTableExportMeta\ExportAdapter::TPT)
        );
        $cmd->setMeta($exportMeta);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath($exportDir)
        );

        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);

        $handler = new ExportTableToFileHandler($this->sessionManager, $this->tableExportQueryBuilderFactory);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames())
        );
        /** @var TableInfo\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableInfo\TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame($sourceTableDef->getColumnsNames(), $columnsNames);

        // check files
        /** @var array<int, array{Key: string, Size: int}> $files */
        $files = $this->listS3BucketDirFiles(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );
        $this->assertNotNull($files);
        $this->assertCount(1, $files);
        if ($expectedResultFileSize !== null) {
            $this->assertGreaterThanOrEqual(
                $expectedResultFileSize[0],
                $files[0]['Size'],
                'File is smaller than expected.'
            );
            $this->assertLessThanOrEqual(
                $expectedResultFileSize[1],
                $files[0]['Size'],
                'File is bigger than expected.'
            );
        }

        // check data
        if ($expectedResultData !== null) {
            $csvData = $this->getObjectAsCsvArray($s3Client, $files[0]['Key']);
            $this->assertEqualsArrays(
                $expectedResultData,
                // data are not trimmed because IE lib doesn't do so. TD serves them in raw form prefixed by space
                $csvData
            );
        }
        // check rows count
        if ($expectedRowsCount !== null) {
            $csvData = $this->getObjectAsCsvArray($s3Client, $files[0]['Key']);
            $this->assertCount($expectedRowsCount, $csvData);
        }

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($sourceTableDef->getSchemaName(), $sourceTableDef->getTableName(), $db);
        $db->close();
    }

    /**
     * @dataProvider slicedExportProvider
     * @param array<int, array<string, mixed>> $expectedFiles
     */
    public function testExportTableToSlicedFile(bool $isCompressed, array $expectedFiles): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export_sliced';
        $exportDir = sprintf(
            'export/%s/',
            md5($this->getName())
        );

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $db);
        $db->close();

        // create table from file
        $this->createSourceTableFromFile(
            $db,
            'export',
            'big_table.csv.gz',
            true,
            $bucketDatabaseName,
            $sourceTableName,
            [
                'FID',
                'NAZEV',
                'Y',
                'X',
                'KONTAKT',
                'SUBKATEGORIE',
                'KATEGORIE',
                'Column6',
                'Column7',
                'Column8',
                'Column9',
                'GlobalID',
            ]
        );

        // clear files
        $s3Client = $this->getS3Client(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION')
        );
        $this->clearS3BucketDir(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $cmd->setFileProvider(FileProvider::S3);

        $cmd->setFileFormat(FileFormat::CSV);

        $exportOptions = new ExportOptions();
        $exportOptions->setIsCompressed($isCompressed);
        $cmd->setExportOptions($exportOptions);

        $exportMeta = new Any();
        $exportMeta->pack(
            (new TableExportToFileCommand\TeradataTableExportMeta())
                ->setExportAdapter(TableExportToFileCommand\TeradataTableExportMeta\ExportAdapter::TPT)
        );
        $cmd->setMeta($exportMeta);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath($exportDir)
        );

        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);

        $handler = new ExportTableToFileHandler($this->sessionManager, $this->tableExportQueryBuilderFactory);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        // check files
        $files = $this->listS3BucketDirFiles(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );
        $this->assertNotNull($files);
        self::assertFilesMatch($expectedFiles, $files);

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $db);
        $db->close();
    }

    public function simpleExportProvider(): Generator
    {
        yield 'plain csv' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                ]),
            ],
            [123, 123], // expected bytes
            [ // expected data
                ['1', '2', '4', '2022-01-01 12:00:01.000000'],
                ['2', '3', '4', '2022-01-02 12:00:02.000000'],
                ['3', '3', '3', '2022-01-03 12:00:03.000000'],
            ],
        ];
        yield 'gzipped csv' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => true,
                ]),
            ],
            [70, 80], // expected bytes
            null, // expected data - it's gzip file, not csv
        ];
        yield 'filter columns' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1', 'col2'],
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['1', '2'],
                ['2', '3'],
                ['3', '3'],
            ],
        ];
        // TODO unable to use ORDER BY because of error: Row size or Sort Key size overflow
//        yield 'filter order by' => [
//            [ // input
//                'exportOptions' => new ExportOptions([
//                    'isCompressed' => false,
//                    'columnsToExport' => ['col1', 'col2'],
//                    'orderBy' => [
//                        new OrderBy([
//                            'columnName' => 'col1',
//                            'order' => Order::DESC,
//                        ]),
//                    ],
//                ]),
//            ],
//            null, // expected bytes
//            [ // expected data
//                ['3'],
//                ['2'],
//                ['1'],
//            ],
//        ];
        // TODO unable to use ORDER BY because of error: Row size or Sort Key size overflow
//        yield 'filter order by with dataType' => [
//            [ // input
//                'exportOptions' => new ExportOptions([
//                    'isCompressed' => false,
//                    'columnsToExport' => ['col1'],
//                    'orderBy' => [
//                        new OrderBy([
//                            'columnName' => 'col1',
//                            'order' => Order::DESC,
//                            'dataType' => DataType::INTEGER,
//                        ]),
//                    ],
//                ]),
//            ],
//            null, // expected bytes
//            [ // expected data
//                ['3'],
//                ['2'],
//                ['1'],
//            ],
//        ];
        // TODO add ORDER BY after it works and fill in expected data
        yield 'filter limit' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'limit' => 2,
                    ]),
                ]),
            ],
            null, // expected bytes
            null, // expected data - result rows are unsorted, so only count rows
            2, // expected rows count
        ];
        yield 'filter changedSince + changedUntil' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1', '_timestamp'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'changeSince' => '1641038401',
                        'changeUntil' => '1641038402',
                    ]),
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['1', '2022-01-01 12:00:01.000000'],
            ],
        ];
        yield 'filter simple where' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::ge,
                                'values' => ['3'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['2'],
                ['3'],
            ],
        ];
        yield 'filter simple where with multiple values' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::eq,
                                'values' => ['3', '4'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['2'],
                ['3'],
            ],
        ];
        yield 'filter multiple where' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::ge,
                                'values' => ['3'],
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'col3',
                                'operator' => Operator::lt,
                                'values' => ['4'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['3'],
            ],
        ];
        yield 'filter where with dataType' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::gt,
                                'values' => ['2.9'],
                                'dataType' => DataType::REAL,
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::lt,
                                'values' => ['3.1'],
                                'dataType' => DataType::REAL,
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'col3',
                                'operator' => Operator::eq,
                                'values' => ['4'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            null, // expected bytes
            [ // expected data
                ['2'],
            ],
        ];
    }

    public function slicedExportProvider(): Generator
    {
        yield 'plain csv' => [
            false, // compression
            [
                ['fileName' => 'F00000', 'size' => 48],
                ['fileName' => 'F00001', 'size' => 43],
            ],
        ];
        yield 'gzipped csv' => [
            true, // compression
            [
                ['fileName' => 'F00000', 'size' => 0],
            ],
        ];
    }

    private function createSourceTable(
        string $databaseName,
        string $tableName,
        Connection $db
    ): TeradataTableDefinition {
        $tableDef = new TeradataTableDefinition(
            $databaseName,
            $tableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
                TeradataColumn::createTimestampColumn(),
            ]),
            ['col1'],
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        // init some values
        foreach ([
                     ['1', '2', '4', '2022-01-01 12:00:01'],
                     ['2', '3', '4', '2022-01-02 12:00:02'],
                     ['3', '3', '3', '2022-01-03 12:00:03'],
                 ] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($databaseName),
                TeradataQuote::quoteSingleIdentifier($tableName),
                implode(',', array_map(
                    static fn($val) => TeradataQuote::quote($val),
                    $i,
                )),
            ));
        }

        return $tableDef;
    }

    private function dropSourceTable(
        string $databaseName,
        string $tableName,
        Connection $db
    ): void {
        if (!$this->isTableExists($db, $databaseName, $tableName)) {
            return;
        }
        $qb = new TeradataTableQueryBuilder();
        $db->executeStatement(
            $qb->getDropTableCommand($databaseName, $tableName)
        );
    }

    /**
     * @param string[] $sourceColumns
     */
    private function createSourceTableFromFile(
        Connection $db,
        string $sourceFilePath,
        string $sourceFileName,
        bool $sourceFileIsCompressed,
        string $destinationDatabaseName,
        string $destinationTableName,
        array $sourceColumns
    ): void {
        // create table
        $columnsLines = [];
        foreach ($sourceColumns as $column) {
            $columnsLines[] = sprintf(
                '%s VARCHAR(500) CHARACTER SET UNICODE',
                $column
            );
        }
        $db->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s (
                    %s
                );',
                TeradataQuote::quoteSingleIdentifier($destinationDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(",\n", $columnsLines)
            )
        );
        $db->close();

        // import data to table
        $cmd = new TableImportFromFileCommand();
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);

        $columns = new RepeatedField(GPBType::STRING);
        foreach ($sourceColumns as $column) {
            $columns[] = $column;
        }
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression($sourceFileIsCompressed
                    ? TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP
                    : TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath($sourceFilePath)
                ->setFileName($sourceFileName)
        );

        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $destinationDatabaseName;
        $cmd->setDestination(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportExportShared\ImportOptions())
                ->setImportType(ImportExportShared\ImportOptions\ImportType::FULL)
                ->setDedupType(ImportExportShared\ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
        );

        $meta = new Any();
        $meta->pack(
            (new TableImportFromFileCommand\TeradataTableImportMeta())
                ->setImportAdapter(TableImportFromFileCommand\TeradataTableImportMeta\ImportAdapter::TPT)
        );
        $cmd->setMeta($meta);

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
    }

    /**
     * @param array<int, array<string, mixed>> $expectedFiles
     * @param array<int, array<string, mixed>> $files
     */
    public static function assertFilesMatch(array $expectedFiles, array $files): void
    {
        self::assertCount(count($expectedFiles), $files);
        /** @var array{fileName: string, size: int} $expectedFile */
        foreach ($expectedFiles as $i => $expectedFile) {
            /** @var array{Key: string, Size: string} $actualFile */
            $actualFile = $files[$i];
            self::assertStringContainsString((string) $expectedFile['fileName'], (string) $actualFile['Key']);
            $fileSize = (int) $actualFile['Size'];
            $expectedFileSize = ((int) $expectedFile['size']) * 1024 * 1024;
            // check that the file size is in range xMB +- 1 000 000B
            //  - (because I cannot really say what the exact size in bytes should be)
            if ($expectedFileSize !== 0) {
                self::assertTrue(
                    ($expectedFileSize - 1000000) < $fileSize && $fileSize < ($expectedFileSize + 100000),
                    sprintf('Actual size is %s but expected is %s', $fileSize, $expectedFileSize)
                );
            }
        }
    }

    /**
     * @return array<mixed>
     */
    private function getObjectAsCsvArray(S3Client $s3Client, string $key): array
    {
        /** @var array{Body: resource} $file */
        $file = $s3Client->getObject([
            'Bucket' => (string) getenv('AWS_S3_BUCKET'),
            'Key' => $key,
        ]);

        $csvData = array_map('str_getcsv', explode(PHP_EOL, (string) $file['Body']));
        array_pop($csvData);
        return $csvData;
    }
}
