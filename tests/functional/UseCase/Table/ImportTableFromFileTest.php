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
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use LogicException;
use Throwable;

class ImportTableFromFileTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse,] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    /**
     * @return Generator<string,array{int}>
     */
    public function importAdapterProvider(): Generator
    {
        yield 'TPT' => [
            TableImportFromFileCommand\TeradataTableImportMeta\ImportAdapter::TPT,
        ];
        yield 'SPT' => [
            TableImportFromFileCommand\TeradataTableImportMeta\ImportAdapter::SPT,
        ];
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableIncrementalLoad(int $importAdapter): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::S3);
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import is not implemented');
            //$ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 2 not unique rows from destination + 1 unique row from source
            // + 1 row which is dedup of two duplicates in source and one from destination
            //$this->assertSame(4, $ref->getRowsCount());
            // @todo test updated values
        } catch (LogicException $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    private function createDestinationTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        Connection $db
    ): TeradataTableDefinition {
        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
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
        // init some values
        foreach ([['1', '2', '4', ''], ['2', '3', '4', ''], ['3', '3', '3', '']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $i)
            ));
        }
        return $tableDestDef;
    }

    private function getCmdMeta(int $adapter): Any
    {
        $meta = new Any();
        $meta->pack(
            (new TableImportFromFileCommand\TeradataTableImportMeta())
                ->setImportAdapter($adapter)
        );
        return $meta;
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableFullLoadWithDeduplication(int $importAdapter): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::S3);
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        try {
            $handler = new ImportTableFromFileHandler($this->sessionManager);
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail full load with deduplication is not implemented');
            //$ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // nothing from destination and 3 rows from source dedup to two
            //$this->assertSame(2, $ref->getRowsCount());
            // @todo test updated values
        } catch (LogicException $e) {
            $this->assertSame('Deduplication is not implemented.', $e->getMessage());
        }

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableFullLoadWithoutDeduplication(int $importAdapter): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::S3);
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(['col1', 'col2', 'col3'], iterator_to_array($response->getImportedColumns()));
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableFullLoadSlicedWithoutDeduplication(int $importAdapter): void
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

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('sliced/accounts')
                ->setFileName('S3.accounts.csvmanifest')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'import',
                'isImported',
                'id',
                'idTwitter',
                'name',
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
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    private function createAccountsTable(Connection $db, string $bucketDatabaseName, string $destinationTableName): void
    {
        $db->executeQuery(sprintf(
            'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
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
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableFullLoadSlicedCompressedWithoutDeduplication(int $importAdapter): void
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

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('sliced/accounts-gzip')
                ->setFileName('S3.accounts-gzip.csvmanifest')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'import',
                'isImported',
                'id',
                'idTwitter',
                'name',
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
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // 0 from destination and 3 rows from source
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableIncrementalSlicedWithDeduplication(int $importAdapter): void
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
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('sliced/accounts')
                ->setFileName('S3.accounts.csvmanifest')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import not implemented.');
            //$ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 1 row from destination + 1 row from destination updated + 1 row from slices new
            //$this->assertSame(3, $ref->getRowsCount());
        } catch (Throwable $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }

    /**
     * @dataProvider importAdapterProvider
     */
    public function testImportTableFromTableIncrementalSlicedCompressedWithDeduplication(int $importAdapter): void
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
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
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
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath('sliced/accounts-gzip')
                ->setFileName('S3.accounts-gzip.csvmanifest')
        );
        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );
        $cmd->setMeta($this->getCmdMeta($importAdapter));

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import not implemented.');
            //$ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 1 row from destination + 1 row from destination updated + 1 row from slices new
            //$this->assertSame(3, $ref->getRowsCount());
        } catch (Throwable $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new TeradataTableQueryBuilder();
        $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $db->close();
    }
}
