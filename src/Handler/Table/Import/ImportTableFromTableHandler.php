<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Import;

use Doctrine\DBAL\Connection;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Exception\ExceptionResolver;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Throwable;

class ImportTableFromTableHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableImportFromTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromTableCommand);

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        // validate
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null, 'TableImportFromFileCommand.source is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'TableImportFromFileCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'TableImportFromFileCommand.importOptions is required.');

        $db = $this->manager->createSession($credentials);

        $source = $this->createSource($db, $command);
        $teradataImportOptions = $this->createOptions($importOptions, $credentials);

        $stagingTable = null;
        $db = $this->manager->createSession($credentials);
        try {
            [
                $stagingTable,
                $importResult,
            ] = $this->import(
                $db,
                $destination,
                $importOptions,
                $source,
                $teradataImportOptions,
                $sourceMapping
            );
        } catch (\Throwable $e){
            throw ExceptionResolver::resolveException($e);
        }
        finally {
            if ($stagingTable !== null) {
                try {
                    $db->executeStatement(
                        (new TeradataTableQueryBuilder())->getDropTableCommand(
                            $stagingTable->getSchemaName(),
                            $stagingTable->getTableName()
                        )
                    );
                } catch (Throwable $e) {
                    // ignore
                }
            }
        }

        $response = new TableImportResponse();
        $destinationRef = new TeradataTableReflection(
            $db,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        );
        $destinationStats = $destinationRef->getTableStats();
        $response->setTableRowsCount($destinationStats->getRowsCount());
        $response->setTableSizeBytes($destinationStats->getDataSizeBytes());
        $response->setImportedColumns(ProtobufHelper::arrayToRepeatedString($importResult->getImportedColumns()));
        $response->setImportedRowsCount($importResult->getImportedRowsCount());
        $timers = new RepeatedField(GPBType::MESSAGE, TableImportResponse\Timer::class);
        foreach ($importResult->getTimers() as $timerArr) {
            $timer = new TableImportResponse\Timer();
            $timer->setName($timerArr['name']);
            $timer->setDuration($timerArr['durationSeconds']);
            $timers[] = $timer;
        }
        $response->setTimers($timers);

        return $response;
    }

    private function createSource(
        Connection $db,
        TableImportFromTableCommand $command
    ): Table {
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null);
        $sourceColumns = [];
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
        foreach ($sourceMapping->getColumnMappings() as $mapping) {
            $sourceColumns[] = $mapping->getSourceColumnName();
        }
        $sourceTableDef = (new TeradataTableReflection(
            $db,
            ProtobufHelper::repeatedStringToArray($sourceMapping->getPath())[0],
            $sourceMapping->getTableName()
        ))->getTableDefinition();
        return new Table(
            ProtobufHelper::repeatedStringToArray($sourceMapping->getPath())[0],
            $sourceMapping->getTableName(),
            $sourceColumns,
            $sourceTableDef->getPrimaryKeysNames()
        );
    }

    private function createOptions(
        ImportOptions $options,
        GenericBackendCredentials $credentials
    ): TeradataImportOptions {
        $strategyMapping = [
            ImportStrategy::STRING_TABLE => ImportOptionsInterface::USING_TYPES_STRING,
            ImportStrategy::USER_DEFINED_TABLE => ImportOptionsInterface::USING_TYPES_USER
        ];
        return new TeradataImportOptions(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            $credentials->getPort(),
            ProtobufHelper::repeatedStringToArray($options->getConvertEmptyValuesToNullOnColumns()),
            $options->getImportType() === ImportType::INCREMENTAL,
            $options->getTimestampColumn() === '_timestamp',
            $options->getNumberOfIgnoredLines(),
            $strategyMapping[$options->getImportStrategy()]
        );
    }

    /**
     * @return array{0: TeradataTableDefinition|null, 1: Result}
     */
    private function import(
        Connection $db,
        CommandDestination $destination,
        ImportOptions $options,
        Table $source,
        TeradataImportOptions $importOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping
    ): array {
        /** @var TeradataTableDefinition $destinationDefinition */
        $destinationDefinition = (new TeradataTableReflection(
            $db,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        ))->getTableDefinition();
        $dedupColumns = ProtobufHelper::repeatedStringToArray($options->getDedupColumnsNames());

        if ($options->getImportType() === ImportType::INCREMENTAL && count($dedupColumns) !== 0) {
            if ($options->getDedupType() === ImportOptions\DedupType::INSERT_DUPLICATES) {
                $dedupColumns = [];
            }
            $destinationDefinition = new TeradataTableDefinition(
                $destinationDefinition->getSchemaName(),
                $destination->getTableName(),
                $destinationDefinition->isTemporary(),
                $destinationDefinition->getColumnsDefinitions(),
                $dedupColumns
            );
        }

        $isFullImport = $options->getImportType() === ImportType::FULL;
        $insertOnlyDuplicates = $options->getDedupType() === ImportOptions\DedupType::INSERT_DUPLICATES;
        if ($isFullImport && $insertOnlyDuplicates && !$importOptions->useTimestamp()) {
            // when full load is performed with no deduplication only copy data using ToStage class
            // this will skip moving data to stage table
            // this is used on full load into workspace where data are deduplicated already
            $toStageImporter = new ToStageImporter($db);
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $destinationDefinition,
                $importOptions
            );
            return [null, $importState->getResult()];
        }

        $stagingTable = $this->createStagingTable($destinationDefinition, $sourceMapping, $db);
        // load to staging table
        $toStageImporter = new ToStageImporter($db);
        $importState = $toStageImporter->importToStagingTable(
            $source,
            $stagingTable,
            $importOptions
        );
        // import data to destination
        $toFinalTableImporter = $importOptions->isIncremental() ? new IncrementalImporter($db) : new FullImporter($db);
        $importResult = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destinationDefinition,
            $importOptions,
            $importState
        );
        return [$stagingTable, $importResult];
    }

    private function createStagingTable(
        TeradataTableDefinition $destinationDefinition,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        Connection $db
    ): TeradataTableDefinition {
        // prepare staging table definition
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = iterator_to_array($sourceMapping->getColumnMappings()->getIterator());
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithMapping(
            $destinationDefinition,
            $mappings
        );
        // create staging table
        $qb = new TeradataTableQueryBuilder();
        $db->executeStatement(
            $qb->getCreateTableCommand(
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName(),
                $stagingTable->getColumnsDefinitions(),
                [] //<-- dont create stage table with primary keys to allow duplicates
                // @todo maybe set primary keys if source table has them
            )
        );
        return $stagingTable;
    }
}
