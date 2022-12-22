<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\QueryBuilder\ExportQueryBuilderFactory;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_LIMIT = 100;
    public const MAX_LIMIT = 1000;

    private TeradataSessionManager $manager;

    private ExportQueryBuilderFactory $queryBuilderFactory;

    public function __construct(
        TeradataSessionManager $manager,
        ExportQueryBuilderFactory $queryBuilderFactory
    ) {
        $this->manager = $manager;
        $this->queryBuilderFactory = $queryBuilderFactory;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param PreviewTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof PreviewTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'PreviewTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'PreviewTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'PreviewTableCommand.columns is required');

        $this->validateFilters($command);

        try {
            $db = $this->manager->createSession($credentials);
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];
            $columnsDefinitions = (new TeradataTableReflection(
                $db,
                $databaseName,
                $command->getTableName(),
            ))->getColumnsDefinitions();

            // build sql
            $queryBuilder = $this->queryBuilderFactory->create($db);
            $queryData = $queryBuilder->buildQueryFromCommand(
                $command->getFilters(),
                $command->getOrderBy(),
                $command->getColumns(),
                $columnsDefinitions,
                $databaseName,
                $command->getTableName(),
                true
            );

            // select table
            $result = $db->executeQuery(
                $queryData->getQuery(),
                $queryData->getBindings(),
                $queryData->getTypes(),
            );

            // set response
            $response = new PreviewTableResponse();

            // set column names
            $response->setColumns($command->getColumns());

            // set rows
            $rows = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row::class);
            foreach ($result->iterateAssociative() as $row) {
                $responseRow = new PreviewTableResponse\Row();
                $responseRowColumns = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row\Column::class);
                /** @var string $columnName */
                foreach ($command->getColumns() as $columnName) {
                    $value = new Value();
                    /** @var ?scalar $columnValue */
                    $columnValue = array_shift($row);
                    if ($columnValue === null) {
                        $value->setNullValue(NullValue::NULL_VALUE);
                    } else {
                        $value->setStringValue((string) $columnValue);
                    }

                    $responseRowColumns[] = (new PreviewTableResponse\Row\Column())
                        ->setColumnName($columnName)
                        ->setValue($value)
                        ->setIsTruncated(array_shift($row) === '1');
                }
                $responseRow->setColumns($responseRowColumns);
                $rows[] = $responseRow;
            }
            $response->setRows($rows);
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }
        return $response;
    }

    private function validateFilters(PreviewTableCommand $command): void
    {
        // build sql
        $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
        assert($columns === array_unique($columns), 'PreviewTableCommand.columns has non unique names');

        $filters = $command->getFilters();
        if ($filters !== null) {
            assert($filters->getLimit() <= self::MAX_LIMIT, 'PreviewTableCommand.limit cannot be greater than 1000');
            if ($filters->getLimit() === 0) {
                $filters->setLimit(self::DEFAULT_LIMIT);
            }

            if ($filters->getChangeSince() !== '') {
                assert(
                    is_numeric($filters->getChangeSince()),
                    'PreviewTableCommand.changeSince must be numeric timestamp'
                );
            }
            if ($filters->getChangeUntil() !== '') {
                assert(
                    is_numeric($filters->getChangeUntil()),
                    'PreviewTableCommand.changeUntil must be numeric timestamp'
                );
            }
        }

        /**
         * @var int $index
         * @var ExportOrderBy $orderBy
         */
        foreach ($command->getOrderBy() as $index => $orderBy) {
            assert($orderBy->getColumnName() !== '', sprintf(
                'PreviewTableCommand.orderBy.%d.columnName is required',
                $index,
            ));
        }
    }
}
