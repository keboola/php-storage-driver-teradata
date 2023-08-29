<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Doctrine\DBAL\Connection;
use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\DedupType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\StorageHelper\StorageTrait;
use Keboola\StorageDriver\Teradata\Handler\Exception\NoSpaceException;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Throwable;

class ImportTableFromFileTest extends ImportBaseCase
{
    use StorageTrait;

    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;
    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;
        $this->projectResponse = $projectResponse;

        [$bucketResponse,] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }


    /**
     * @dataProvider isTypedTablesProvider
     */
    public function testImportTableFromTableIncrementalLoad(bool $isTypedTable): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $db);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);
        }

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'import', 'a_b_c-3row.csv');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::INCREMENTAL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        // 2 not unique rows from destination + 1 unique row from source
        // + 1 row which is dedup of two duplicates in source and one from destination
        $this->assertIDsInLoadedTable($db, $bucketDatabaseName, $destinationTableName, ['1', '2', '3', '5'], 'col1');

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * @dataProvider isTypedTablesProvider
     */
    public function testImportTableFromTableFullLoadWithDeduplication(bool $isTypedTable): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $db);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);
        }

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'import', 'a_b_c-3row.csv');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::FULL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source -> dedup to two
        $this->assertSame(2, $ref->getRowsCount());

        $data = $db->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s ORDER BY %s',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                TeradataQuote::quoteSingleIdentifier('col1')
            )
        );

        // assert values but skip timestamp
        $this->assertEqualsArrays(['col1' => '1', 'col2' => '2', 'col3' => '3'], array_slice($data[0], 0, 3));
        $this->assertEqualsArrays(['col1' => '5', 'col2' => '2', 'col3' => '3'], array_slice($data[1], 0, 3));

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * @dataProvider isTypedTablesProvider
     */
    public function testImportTableFromTableFullLoadWithoutDeduplication(bool $isTypedTable): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $db);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);
        }

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'import', 'a_b_c-3row.csv');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::FULL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(['col1', 'col2', 'col3'], iterator_to_array($response->getImportedColumns()));
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());
        $this->assertIDsInLoadedTable($db, $bucketDatabaseName, $destinationTableName, ['1', '1', '5'], 'col1');

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    private function createAccountsTable(Connection $db, string $bucketDatabaseName, string $destinationTableName): void
    {
        $db->executeQuery(sprintf(
            'CREATE MULTISET TABLE %s.%s (
                "id" VARCHAR(50) CHARACTER SET UNICODE,
                "idTwitter" VARCHAR(50) CHARACTER SET UNICODE,
                "name" VARCHAR(100) CHARACTER SET UNICODE,
                "import" VARCHAR(50) CHARACTER SET UNICODE,
                "isImported" VARCHAR(50) CHARACTER SET UNICODE,
                "apiLimitExceededDatetime" VARCHAR(50) CHARACTER SET UNICODE,
                "analyzeSentiment" VARCHAR(50) CHARACTER SET UNICODE,
                "importKloutScore" VARCHAR(50) CHARACTER SET UNICODE,
                "timestamp" VARCHAR(50) CHARACTER SET UNICODE,
                "oauthToken" VARCHAR(50) CHARACTER SET UNICODE,
                "oauthSecret" VARCHAR(50) CHARACTER SET UNICODE,
                "idApp" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            ) PRIMARY INDEX ("id");',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName)
        ));
    }

    /**
     * @return Generator<string,array{int}>
     */
    public function importCompressionProvider(): Generator
    {
            yield 'NO Compression ' => [
                TableImportFromFileCommand\CsvTypeOptions\Compression::NONE,
            ];
            yield 'GZIP ' => [
                TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP,
            ];
    }

    /**
     * @dataProvider importCompressionProvider
     */
    public function testImportTableFromTableFullLoadSlicedWithoutDeduplication(
        int $compression
    ): void {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        $this->createAccountsTable($db, $bucketDatabaseName, $destinationTableName);
        // init some values
        $db->executeStatement(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (10,448810375,\'init\',0,1,,1,0,\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',1,\'2012-02-20 09:34:22\')',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName),
        ));

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'apiLimitExceededDatetime';
        $columns[] = 'analyzeSentiment';
        $columns[] = 'importKloutScore';
        $columns[] = 'timestamp';
        $columns[] = 'oauthToken';
        $columns[] = 'oauthSecret';
        $columns[] = 'idApp';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE)
                ->setCompression($compression)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        if ($compression === TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP) {
            $this->setFilePathAndCredentials(
                $cmd,
                'sliced/accounts-gzip',
                '%MANIFEST_PREFIX%accounts-gzip.csvmanifest',
            );
        } else {
            // no compression
            $this->setFilePathAndCredentials(
                $cmd,
                'sliced/accounts',
                '%MANIFEST_PREFIX%accounts.csvmanifest',
            );
        }
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::FULL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'id',
                'idTwitter',
                'name',
                'import',
                'isImported',
                'apiLimitExceededDatetime',
                'analyzeSentiment',
                'importKloutScore',
                'timestamp',
                'oauthToken',
                'oauthSecret',
                'idApp',
            ],
            iterator_to_array($response->getImportedColumns())
        );
        // 0 (id=10 was overwritten by fullload) from destination and 3 rows from source
        $this->assertIDsInLoadedTable($db, $bucketDatabaseName, $destinationTableName, ['15', '18', '60']);

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    public function testImportTableFromTableIncrementalSlicedWithDeduplication(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        $this->createAccountsTable($db, $bucketDatabaseName, $destinationTableName);
        // init some values
        $db->executeStatement(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (10,448810375,\'init\',0,1,,1,0,\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',1,\'2012-02-20 09:34:22\')',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName),
        ));
        // this line should be updated
        $db->executeStatement(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (15,44,\'init replace\',0,1,,1,0,\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',1,\'2012-02-20 09:34:22\')',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName),
        ));

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'apiLimitExceededDatetime';
        $columns[] = 'analyzeSentiment';
        $columns[] = 'importKloutScore';
        $columns[] = 'timestamp';
        $columns[] = 'oauthToken';
        $columns[] = 'oauthSecret';
        $columns[] = 'idApp';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'sliced/accounts', '%MANIFEST_PREFIX%accounts.csvmanifest');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'id';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::INCREMENTAL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        // 1 row from destination (10) + 1 row from destination updated (15) + 1 row from first slice (18)
        // + 1 row from second slice (60)
        $this->assertIDsInLoadedTable($db, $bucketDatabaseName, $destinationTableName, ['10', '15', '18', '60']);

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    public function testImportTableFromTableIncrementalSlicedCompressedWithDeduplication(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        $this->createAccountsTable($db, $bucketDatabaseName, $destinationTableName);
        // init some values
        $db->executeStatement(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (10,448810375,\'init\',0,1,,1,0,\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',1,\'2012-02-20 09:34:22\')',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName),
        ));
        // this line should be updated
        $db->executeStatement(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (15,44,\'init replace\',0,1,,1,0,\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',1,\'2012-02-20 09:34:22\')',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($destinationTableName),
        ));

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'apiLimitExceededDatetime';
        $columns[] = 'analyzeSentiment';
        $columns[] = 'importKloutScore';
        $columns[] = 'timestamp';
        $columns[] = 'oauthToken';
        $columns[] = 'oauthSecret';
        $columns[] = 'idApp';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'sliced/accounts-gzip', '%MANIFEST_PREFIX%accounts-gzip.csvmanifest');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'id';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::INCREMENTAL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        // 1 row from destination + 1 row from destination updated + 1 row from first slice, 1 row from the second one
        $this->assertIDsInLoadedTable($db, $bucketDatabaseName, $destinationTableName, ['10', '15', '18', '60']);
        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    /**
     * @param string[] $expectedIds
     */
    protected function assertIDsInLoadedTable(
        Connection $db,
        string $dbName,
        string $tableName,
        array $expectedIds,
        string $columnName = 'id'
    ): void {
        /** @var array{string[]} $data */
        $data = $db->fetchAllAssociative(
            sprintf(
                'SELECT %s FROM %s.%s ORDER BY %s',
                TeradataQuote::quoteSingleIdentifier($columnName),
                TeradataQuote::quoteSingleIdentifier($dbName),
                TeradataQuote::quoteSingleIdentifier($tableName),
                TeradataQuote::quoteSingleIdentifier($columnName)
            )
        );

        $this->assertEqualsArrays($expectedIds, array_map(fn($item) => trim($item[$columnName]), $data));
    }

    public function testImportBigDataToSmallBucket(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';

        // phpcs:ignore
        $columnNames = ['FID', 'NAZEV', 'Y', 'X', 'KONTAKT', 'SUBKATEGORIE', 'KATEGORIE', 'Column6', 'Column7', 'Column8', 'Column9', 'GlobalID'];

        // tested bucket has 100MB, imported CSV has 86MB -> it loads data to staging (takes 86MB) table
        // and then to final (trying to allocate another 86MB => fail)
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // init destination table with structure of big_table.csv
        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                ...array_map(fn($colName) => TeradataColumn::createGenericColumn($colName), $columnNames),
                TeradataColumn::createGenericColumn('_timestamp'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        // command for file import
        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);

        foreach ($columnNames as $name) {
            $columns[] = $name;
        }

        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $this->setFilePathAndCredentials($cmd, 'export', 'big_table.csv.gz');
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::FULL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setImportStrategy(ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(),
            );
        } catch (Throwable $e) {
            $this->assertInstanceOf(NoSpaceException::class, $e);
            $this->assertEquals(ExceptionInterface::ERR_RESOURCE_FULL, $e->getCode());
            $this->assertEquals('Database is full. Cannot insert data or create new objects.', $e->getMessage());
        }

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }
}
