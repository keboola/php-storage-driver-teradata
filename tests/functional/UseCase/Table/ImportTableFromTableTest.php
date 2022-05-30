<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use DateTime;
use Doctrine\DBAL\Connection;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use LogicException;

class ImportTableFromTableTest extends BaseCase
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
            []
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
            []
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
     */
    public function testImportTableFromTableIncrementalLoad(): void
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
                new TeradataColumn(
                    'col1',
                    new Teradata(Teradata::TYPE_VARCHAR, [
                        'length' => '32000',
                        'nullable' => false,
                    ])
                ),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
            ]),
            ['col1']
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [], //<-- dont create primary keys allow duplicates
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

        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new TeradataColumn(
                    'col1',
                    new Teradata(Teradata::TYPE_VARCHAR, [
                        'length' => '32000',
                        'nullable' => false,
                    ])
                ),
                TeradataColumn::createGenericColumn('col4'), // <- different col rename
                TeradataColumn::createGenericColumn('_timestamp'),
            ]),
            ['col1']
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $i)
            ));
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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import is not implemented');
            //$ref = new TeradataTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 1 row unique from source, 3 rows deduped from source and destination
            //$this->assertSame(4, $ref->getRowsCount());
            //$this->assertTimestamp($db, $bucketDatabaseName, $destinationTableName);
            // @todo test updated values
        } catch (LogicException $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

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
}
