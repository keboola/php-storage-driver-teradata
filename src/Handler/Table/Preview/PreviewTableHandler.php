<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableFilterQueryBuilderFactory;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    // TODO truncated: orezat primo v query na 16384 znaku (viz ExasolExportQueryBuilder::processSelectStatement)
    //   (ale je tam zaroven podminka, ze exportni format musi byt JSON)
    public const STRING_MAX_LENGTH = 50;

    public const DEFAULT_LIMIT = 100;
    public const MAX_LIMIT = 1000;

    public const ALLOWED_DATA_TYPES = [
        DataType::INTEGER => Teradata::TYPE_INTEGER,
        DataType::BIGINT => Teradata::TYPE_BIGINT,
        DataType::REAL => Teradata::TYPE_REAL,
    ];

    private TeradataSessionManager $manager;
    private TableFilterQueryBuilderFactory $queryBuilderFactory;

    public function __construct(
        TeradataSessionManager $manager,
        TableFilterQueryBuilderFactory $queryBuilderFactory
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

        try {
            $db = $this->manager->createSession($credentials);
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];

            // validate
            $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
            assert($columns === array_unique($columns), 'PreviewTableCommand.columns has non unique names');
            if ($command->getLimit() === 0) {
                $command->setLimit(self::DEFAULT_LIMIT);
            }
            if ($command->getLimit() > self::MAX_LIMIT) {
                $command->setLimit(self::MAX_LIMIT);
            }
            if ($command->hasOrderBy() && $command->getOrderBy()) {
                /** @var PreviewTableCommand\PreviewTableOrderBy $orderBy */
                $orderBy = $command->getOrderBy();
                assert($orderBy->getColumnName() !== '', 'PreviewTableCommand.orderBy.columnName is required');
            }

            // build sql
            $tableInfo = $this->getTableInfoResponseIfNeeded($credentials, $command, $databaseName);
            $queryBuilder = $this->queryBuilderFactory->create($db, $tableInfo);
            $queryData = $queryBuilder->buildQueryFromCommand($command, $databaseName);

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
            foreach ($result->iterateAssociative() as $line) {
                // set row
                $row = new PreviewTableResponse\Row();
                $rowColumns = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row\Column::class);
                /** @var ?scalar $itemValue */
                foreach ($line as $itemKey => $itemValue) {
                    // set row columns
                    $value = new Value();
                    $truncated = false;
                    if ($itemValue === null) {
                        $value->setNullValue(NullValue::NULL_VALUE);
                    } else {
                        // preview returns all data as string
                        // TODO truncated: rewrite to SQL
                        if (mb_strlen((string) $itemValue) > self::STRING_MAX_LENGTH) {
                            $truncated = true;
                            $value->setStringValue(mb_substr((string) $itemValue, 0, self::STRING_MAX_LENGTH));
                        } else {
                            $value->setStringValue((string) $itemValue);
                        }
                    }

                    $rowColumns[] = (new PreviewTableResponse\Row\Column())
                        ->setColumnName($itemKey)
                        ->setValue($value)
                        ->setIsTruncated($truncated);

                    $row->setColumns($rowColumns);
                }
                $rows[] = $row;
            }
            $response->setRows($rows);
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }
        return $response;
    }

    private function applyDataType(string $columnName, int $dataType): string
    {
        if ($dataType === DataType::STRING) {
            return $columnName;
        }
        if (!array_key_exists($dataType, self::ALLOWED_DATA_TYPES)) {
            $allowedTypesList = [];
            foreach (self::ALLOWED_DATA_TYPES as $typeId => $typeName) {
                $allowedTypesList[] = sprintf('%s for %s', $typeId, $typeName);
            }
            throw new Exception(
                sprintf(
                    'Data type %s not recognized. Possible datatypes are [%s]',
                    $dataType,
                    implode('|', $allowedTypesList)
                )
            );
        }
        return sprintf(
            'CAST(%s AS %s)',
            $columnName,
            self::ALLOWED_DATA_TYPES[$dataType]
        );
    }

    /**
     * fulltext search need table info data
     */
    private function getTableInfoResponseIfNeeded(
        GenericBackendCredentials $credentials,
        PreviewTableCommand $command,
        string $databaseName
    ): ?TableInfo {
        if ($command->getFulltextSearch() !== '') {
            $objectInfoHandler = (new ObjectInfoHandler($this->manager));
            $tableInfoCommand = (new ObjectInfoCommand())
                ->setExpectedObjectType(ObjectType::TABLE)
                ->setPath(ProtobufHelper::arrayToRepeatedString([
                    $databaseName,
                    $command->getTableName()
                ]));
            /** @var ObjectInfoResponse $response */
            $response = $objectInfoHandler($credentials, $tableInfoCommand, []);

            assert($response instanceof ObjectInfoResponse);
            assert($response->getObjectType() === ObjectType::TABLE);

            return $response->getTableInfo();
        }
        return null;
    }
}
