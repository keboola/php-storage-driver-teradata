<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Import;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromFileResponse;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\MetaHelper;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use LogicException;
use Throwable;

class ImportTableFromFileHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableImportFromFileCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromFileCommand);

        // validate
        assert(
            $command->getFileProvider() === FileProvider::S3,
            'Only S3 is supported TableImportFromFileCommand.fileProvider.'
        );
        assert(
            $command->getFileFormat() === FileFormat::CSV,
            'Only CSV is supported TableImportFromFileCommand.fileFormat.'
        );
        $any = $command->getFormatTypeOptions();
        assert($any !== null, 'TableImportFromFileCommand.formatTypeOptions is required.');
        $formatOptions = $any->unpack();
        assert($formatOptions instanceof TableImportFromFileCommand\CsvTypeOptions);
        assert(
            $formatOptions->getSourceType() !== TableImportFromFileCommand\CsvTypeOptions\SourceType::DIRECTORY,
            'TableImportFromFileCommand.formatTypeOptions.sourceType directory is not supported.'
        );
        assert($command->hasFilePath() === true, 'TableImportFromFileCommand.filePath is required.');
        $any = $command->getFileCredentials();
        assert($any !== null, 'TableImportFromFileCommand.fileCredentials is required.');
        $fileCredentials = $any->unpack();
        assert(
            $fileCredentials instanceof S3Credentials,
            'TableImportFromFileCommand.fileCredentials is required to be S3Credentials.'
        );
        $destination = $command->getDestination();
        assert($destination !== null, 'TableImportFromFileCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'TableImportFromFileCommand.importOptions is required.');

        $csvOptions = new CsvOptions(
            $formatOptions->getDelimiter(),
            $formatOptions->getEnclosure(),
            $formatOptions->getEscapedBy()
        );

        $filePath = $command->getFilePath();
        assert($filePath !== null);
        $source = $this->getSourceFile($filePath, $fileCredentials, $csvOptions, $formatOptions);
        $meta = MetaHelper::getMetaFromCommand($command, TableImportFromFileCommand\TeradataTableImportMeta::class);
        assert($meta instanceof TableImportFromFileCommand\TeradataTableImportMeta);
        $teradataImportOptions = $this->createOptions($importOptions, $credentials, $meta);

        $stagingTable = null;
        $db = $this->manager->createSession($credentials);

        if ($importOptions->getImportType() === ImportType::INCREMENTAL) {
            throw new LogicException('Not implemented');
        }
        $destinationRef = new TeradataTableReflection(
            $db,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        );
        try {
            /** @var TeradataTableDefinition $destinationDefinition */
            $destinationDefinition = $destinationRef->getTableDefinition();
            $dedupColumns = ProtobufHelper::repeatedStringToArray($importOptions->getDedupColumnsNames());
            if ($importOptions->getImportType() === ImportOptions\DedupType::UPDATE_DUPLICATES
                && count($dedupColumns) !== 0
            ) {
                // @todo dudupColumns should be pasted to destination table as primary keys to work
                // this should change in import export lib
                //$destinationDefinition = new TeradataTableDefinition(
                //    $destinationDefinition->getSchemaName(),
                //    $destination->getTableName(),
                //    $destinationDefinition->isTemporary(),
                //    $destinationDefinition->getColumnsDefinitions(),
                //    $dedupColumns,
                //);
                throw new LogicException('Deduplication is not implemented.');
            }
            // prepare staging table definition
            $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionForTPT(
                $destinationDefinition,
                $source->getColumnsNames()
            );
            // create staging table
            $qb = new TeradataTableQueryBuilder();
            $db->executeStatement(
                $qb->getCreateTableCommandFromDefinition($stagingTable)
            );
            // load to staging table
            $toStageImporter = new ToStageImporter($db);
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $teradataImportOptions
            );
            // import data to destination
            $toFinalTableImporter = new FullImporter($db);
            //if ($importOptions->getImportType() === ImportType::INCREMENTAL) {
            //    //$toFinalTableImporter = new IncrementalImporter($db);
            //}
            $importResult = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destinationDefinition,
                $teradataImportOptions,
                $importState
            );
        } catch (Throwable $e) {
            throw $e;
        } finally {
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

    private function getSourceFile(
        FilePath $filePath,
        S3Credentials $fileCredentials,
        CsvOptions $csvOptions,
        TableImportFromFileCommand\CsvTypeOptions $formatOptions
    ): SourceFile {
        $relativePath = RelativePath::create(
            new S3Provider(),
            $filePath->getRoot(),
            $filePath->getPath(),
            $filePath->getFileName()
        );
        return new SourceFile(
            $fileCredentials->getKey(),
            $fileCredentials->getSecret(),
            $fileCredentials->getRegion(),
            $relativePath->getRoot(),
            $relativePath->getPathnameWithoutRoot(),
            $csvOptions,
            $formatOptions->getSourceType() === TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE,
            ProtobufHelper::repeatedStringToArray($formatOptions->getColumnsNames()),
            [] // <-- ignore primary keys here should be deprecated
        );
    }

    private function createOptions(
        ImportOptions $options,
        GenericBackendCredentials $credentials,
        ?TableImportFromFileCommand\TeradataTableImportMeta $meta
    ): TeradataImportOptions {
        $adapter = TeradataImportOptions::CSV_ADAPTER_TPT;
        if ($meta !== null) {
            switch ($meta->getImportAdapter()) {
                case TableImportFromFileCommand\TeradataTableImportMeta\ImportAdapter::TPT:
                    $adapter = TeradataImportOptions::CSV_ADAPTER_TPT;
                    break;
                default:
                    throw new LogicException(sprintf(
                        'Unknown CSV import adapter "%s"',
                        $meta->getImportAdapter()
                    ));
            }
        }
        return new TeradataImportOptions(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            $credentials->getPort(),
            ProtobufHelper::repeatedStringToArray($options->getConvertEmptyValuesToNullOnColumns()),
            $options->getImportType() === ImportType::INCREMENTAL,
            $options->getTimestampColumn() === '_timestamp',
            $options->getNumberOfIgnoredLines(),
            $adapter
        );
    }
}
