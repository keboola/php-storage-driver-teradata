<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use DateTime;
use Doctrine\DBAL\Connection;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
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
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\StorageHelper\StorageTrait;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Exception\NoSpaceException;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Throwable;

class ImportTableFromTableTest extends ImportBaseCase
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
     * Full load to workspace simulation
     * This is input mapping, no timestamp is updated
     */
    public function testImportTableFromTableFullLoadNoDedup(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        $tableSourceDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
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
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            ));
        }

        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col4'), // <- different col rename
            ]),
            []
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col4');
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [], // optimized full load is not returning imported columns
            iterator_to_array($response->getImportedColumns())
        );
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * Full load to storage from workspace
     * This is output mapping, timestamp is updated
     */
    public function testImportTableFromTableFullLoadWithTimestamp(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create tables
        $tableSourceDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
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
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            ));
        }

        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col4'), // <- different col rename
                TeradataColumn::createGenericColumn('_timestamp'),
            ]),
            []
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col4');
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);
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
                'col1',
                'col4',
            ],
            iterator_to_array($response->getImportedColumns())
        );
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertTimestamp($db, $bucketDatabaseName, $destinationTableName);

        // cleanup
        $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    /**
     * Incremental load to storage from workspace
     * This is output mapping, timestamp is updated
     * @dataProvider isTypedTablesProvider
     */
    public function testImportTableFromTableIncrementalLoad(bool $isTypedTable): void
    {
        // typed tables have to have same structure, but string tables can do the mapping
        $sourceExtraColumn = $isTypedTable ? 'col3' : 'colX';

        $sourceTableName = md5($this->getName()) . '_Test_table';
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // create source table, basically it imitates user table in workspace - usually typed
        $tableSourceDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new TeradataColumn('col1', new Teradata(
                    Teradata::TYPE_INT,
                    []
                )),
                new TeradataColumn('col2', new Teradata(
                    Teradata::TYPE_BIGINT,
                    []
                )),
                TeradataColumn::createGenericColumn($sourceExtraColumn),
            ]),
            ['col1']
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [] //<-- dont create primary keys allow duplicates
        );
        $db->executeStatement($sql);
        foreach ([['1', '1', '3'], ['2', '2', '2'], ['2', '2', '2'], ['3', '2', '3'], ['4', '4', '4']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            ));
        }

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $db);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $db);
        }

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName($sourceExtraColumn)
            ->setDestinationColumnName('col3');
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'col1';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
        // 1 row unique from source, 3 rows deduped from source and destination
        $this->assertSame(4, $ref->getRowsCount());
        $this->assertTimestamp($db, $bucketDatabaseName, $destinationTableName);

        // cleanup
        $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    private function assertTimestamp(
        Connection $db,
        string $database,
        string $tableName
    ): void {
        /** @var array<int, string> $timestamps */
        $timestamps = $db->fetchFirstColumn(sprintf(
            'SELECT _timestamp FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($database),
            TeradataQuote::quoteSingleIdentifier($tableName)
        ));

        foreach ($timestamps as $timestamp) {
            $this->assertNotEmpty($timestamp);
            $this->assertEqualsWithDelta(
                new DateTime('now'),
                new DateTime($timestamp),
                60 // set to 1 minute, it's important that timestamp is there
            );
        }
    }


    /**
     * Full load to workspace simulation
     * This is input mapping, no timestamp is updated
     */
    public function testImportOfBigTableToSmallBucket(): void
    {
        // 1. create big bucket
        $bigBucket = md5($this->getName()) . '_Test_big_bucket';
        $meta = new Any();
        $meta->pack(
            (new CreateBucketCommand\CreateBucketTeradataMeta())
                ->setPermSpace('200e6') // 200MB
                ->setSpoolSpace('200e6') // 200MB
        );
        $handler = new CreateBucketHandler($this->sessionManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bigBucket)
            ->setMeta($meta)
            ->setProjectRoleName($this->projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $bigBucketResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $bigBucketResponse);

        // 2. and load a big table in it
        $bigTableName = md5($this->getName()) . '_Test_table_final';
        // phpcs:ignore
        $columnNames = ['FID', 'NAZEV', 'Y', 'X', 'KONTAKT', 'SUBKATEGORIE', 'KATEGORIE', 'Column6', 'Column7', 'Column8', 'Column9', 'GlobalID'];

        $bigBucketDatabaseName = $bigBucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        // init destination table with structure of big_table.csv
        $tableDestDef = new TeradataTableDefinition(
            $bigBucketDatabaseName,
            $bigTableName,
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
        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $bigBucketDatabaseName;
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
                ->setPath($sourcePath)
                ->setTableName($bigTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportType::FULL)
                ->setDedupType(DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames(new RepeatedField(GPBType::STRING))
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setImportStrategy(ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        /** @var TableImportResponse $bigBucketResponse */
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );

        // 3. create a small bucket
        $smallBucket = md5($this->getName()) . '_Test_small_bucket';
        $tableNameInSmallBucket = 'targetTable';
        $meta = new Any();
        $meta->pack(
            (new CreateBucketCommand\CreateBucketTeradataMeta())
                ->setPermSpace('1e6') // 1MB
                ->setSpoolSpace('1e6') // 1MB
        );
        $handler = new CreateBucketHandler($this->sessionManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($smallBucket)
            ->setMeta($meta)
            ->setProjectRoleName($this->projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $smallBucketResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $smallBucketResponse);

        // 3.1 init a table in small bucket
        $tableDestDef = new TeradataTableDefinition(
            $smallBucketResponse->getCreateBucketObjectName(),
            $tableNameInSmallBucket,
            false,
            new ColumnCollection(
                array_map(fn($colName) => TeradataColumn::createGenericColumn($colName), $columnNames)
            ),
            []
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db = $this->getConnection($this->projectCredentials);

        $db->executeStatement($sql);

        // 4. load the big table to a small bucket -> it should fail
        $cmd = new TableImportFromTableCommand();
        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $bigBucketDatabaseName;

        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $smallBucketResponse->getCreateBucketObjectName();

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
        );
        foreach ($columnNames as $name) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($name)
                ->setDestinationColumnName($name);
        }
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($bigTableName)
                ->setColumnMappings($columnMappings)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($tableNameInSmallBucket)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);
        /** @var TableImportResponse $bigBucketResponse */
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(),
            );
            $this->fail('should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(NoSpaceException::class, $e);
            $this->assertEquals(ExceptionInterface::ERR_RESOURCE_FULL, $e->getCode());
            $this->assertEquals('Database is full. Cannot insert data or create new objects.', $e->getMessage());
        }
    }
}
