<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Export;

use Google\Protobuf\Internal\Message;
use Keboola\Db\ImportExport\Backend\Teradata\Exporter;
use Keboola\Db\ImportExport\Storage\S3\DestinationFile;
use Keboola\Db\ImportExport\Storage\Teradata\SelectSource;
use Keboola\Db\ImportExport\Storage\Teradata\TeradataExportOptions;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\MetaHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableExportFilterQueryBuilderFactory;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class ExportTableToFileHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_BUFFER_SIZE = '5M';
    public const DEFAULT_MAX_OBJECT_SIZE = '50M';

    private TeradataSessionManager $manager;
    private TableExportFilterQueryBuilderFactory $queryBuilderFactory;

    public function __construct(
        TeradataSessionManager $manager,
        TableExportFilterQueryBuilderFactory $queryBuilderFactory
    )
    {
        $this->manager = $manager;
        $this->queryBuilderFactory = $queryBuilderFactory;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableExportToFileCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableExportToFileCommand);

        // validate
        $source = $command->getSource();
        assert($source !== null, 'TableExportToFileCommand.source is required.');
        assert(
            $source->getPath()->count() === 1,
            'TableExportToFileCommand.source.path is required and size must equal 1'
        );
        assert(!empty($source->getTableName()), 'TableExportToFileCommand.source.tableName is required');

        assert(
            $command->getFileProvider() === FileProvider::S3,
            'Only S3 is supported TableExportToFileCommand.fileProvider.'
        );

        assert(
            $command->getFileFormat() === FileFormat::CSV,
            'Only CSV is supported TableExportToFileCommand.fileFormat.'
        );
        assert($command->getFilePath() !== null, 'TableExportToFileCommand.filePath is required.');

        $any = $command->getFileCredentials();
        assert($any !== null, 'TableExportToFileCommand.fileCredentials is required.');
        $fileCredentials = $any->unpack();
        assert(
            $fileCredentials instanceof S3Credentials,
            'TableExportToFileCommand.fileCredentials is required to be S3Credentials.'
        );

        // validate exportOptions
        $requestExportOptions = $command->getExportOptions();
        $this->validateExportOptions($requestExportOptions);

        $exportOptions = $this->createOptions(
            $credentials,
            $requestExportOptions
        );

        $commandMeta = MetaHelper::getMetaFromCommand(
            $command,
            TableExportToFileCommand\TeradataTableExportMeta::class
        );
        assert($commandMeta !== null, 'TableExportToFileCommand.meta is required.');

        // run
        $db = $this->manager->createSession($credentials);

        $database = ProtobufHelper::repeatedStringToArray($source->getPath())[0];
        $queryBuilder = $this->queryBuilderFactory->create($db);
        $queryData = $queryBuilder->buildQueryFromCommand($command, $database, $source->getTableName());
        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();
        $sql = $queryBuilder::processSqlWithBindingParameters(
            $queryData->getQuery(),
            $queryDataBindings,
            $queryData->getTypes(),
        );

        $sourceRef = new SelectSource(
            $sql,
        );
        $destinationRef = $this->getDestinationFile($command->getFilePath(), $fileCredentials);

        (new Exporter($db))->exportTable(
            $sourceRef,
            $destinationRef,
            $exportOptions
        );

        return (new TableExportToFileResponse())
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $database,
                new TeradataTableReflection(
                    $db,
                    $database,
                    $source->getTableName()
                )
            ));
    }

    private function getDestinationFile(
        FilePath $filePath,
        S3Credentials $fileCredentials
    ): DestinationFile {
        $relativePath = RelativePath::create(
            new S3Provider(),
            $filePath->getRoot(),
            $filePath->getPath(),
            $filePath->getFileName(),
        );
        return new DestinationFile(
            $fileCredentials->getKey(),
            $fileCredentials->getSecret(),
            $fileCredentials->getRegion(),
            $relativePath->getRoot(),
            $relativePath->getPathnameWithoutRoot()
        );
    }

    private function createOptions(
        GenericBackendCredentials $credentials,
        ?ExportOptions $options
    ): TeradataExportOptions {
        return new TeradataExportOptions(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            $credentials->getPort(),
            $options && $options->getIsCompressed(),
            self::DEFAULT_BUFFER_SIZE,
            self::DEFAULT_MAX_OBJECT_SIZE
        );
    }

    private function validateExportOptions(?ExportOptions $requestExportOptions): void
    {
        if ($requestExportOptions) {
            $columnsToExport = ProtobufHelper::repeatedStringToArray($requestExportOptions->getColumnsToExport());
            assert(
                $columnsToExport === array_unique($columnsToExport),
                'TableExportToFileCommand.exportOptions.columnsToExport has non unique names'
            );

            if ($requestExportOptions->getChangeSince() !== '') {
                assert(
                    is_numeric($requestExportOptions->getChangeSince()),
                    'TableExportToFileCommand.exportOptions.changeSince must be numeric timestamp'
                );
            }
            if ($requestExportOptions->getChangeUntil() !== '') {
                assert(
                    is_numeric($requestExportOptions->getChangeUntil()),
                    'TableExportToFileCommand.exportOptions.changeUntil must be numeric timestamp'
                );
            }

            /**
             * @var int $index
             * @var OrderBy $orderBy
             */
            foreach ($requestExportOptions->getOrderBy() as $index => $orderBy) {
                assert($orderBy->getColumnName() !== '', sprintf(
                    'TableExportToFileCommand.exportOptions.orderBy.%d.columnName is required',
                    $index,
                ));
            }
        }
    }
}
