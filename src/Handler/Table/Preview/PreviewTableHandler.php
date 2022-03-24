<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    // TODO nejspis se to ma orezat primo v query na 16384 znaku (viz ExasolExportQueryBuilder::processSelectStatement)
    //   ale je tam zaroven podminka, ze exportni format musi byt JSON -> takze to tady nejspis neni vubec potreba?
    public const STRING_MAX_LENGTH = 50;

    public const MAX_LIMIT = 1000;

    public const ALLOWED_DATA_TYPES = [
        DataType::INTEGER => Teradata::TYPE_INTEGER,
        DataType::BIGINT => Teradata::TYPE_BIGINT,
        DataType::REAL => Teradata::TYPE_REAL,
    ];

    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
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
        assert(!empty($command->getTableName()), 'PreviewTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'PreviewTableCommand.columns is required');

        try {
            $db = $this->manager->createSession($credentials);
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];

            // build sql
            $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
            assert($columns === array_unique($columns), 'PreviewTableCommand.columns has non unique names');
            $columnsSql = implode(', ', array_map([TeradataQuote::class, 'quoteSingleIdentifier'], $columns));

            $limitSql = sprintf(
                'TOP %d',
                ($command->getLimit() > 0 && $command->getLimit() < self::MAX_LIMIT)
                    ? $command->getLimit()
                    : self::MAX_LIMIT
            );

            // TODO changeSince, changeUntil
            // TODO fulltextSearch
            // TODO whereFilters
            $selectTableSql = sprintf(
                "SELECT %s %s\nFROM %s.%s",
                $limitSql,
                $columnsSql,
                TeradataQuote::quoteSingleIdentifier($databaseName),
                TeradataQuote::quoteSingleIdentifier($command->getTableName())
            );

            if ($command->hasOrderBy() && $command->getOrderBy()) {
                /** @var PreviewTableCommand\PreviewTableOrderBy $orderBy */
                $orderBy = $command->getOrderBy();
                assert(!empty($orderBy->getColumnName()), 'PreviewTableCommand.orderBy.columnName is required');
                $quotedColumnName = TeradataQuote::quoteSingleIdentifier($orderBy->getColumnName());
                $selectTableSql .= sprintf(
                    "\nORDER BY %s %s",
                    $this->applyDataType($quotedColumnName, $orderBy->getDataType()),
                    $orderBy->getOrder() === PreviewTableCommand\PreviewTableOrderBy\Order::DESC ? 'DESC' : 'ASC'
                );
            }

            // select table
            $result = $db->executeQuery($selectTableSql);

            // set response
            $response = new PreviewTableResponse();

            // set column names
            $firstLine = $result->fetchAssociative();
            if ($firstLine) {
                $columns = new RepeatedField(GPBType::STRING);
                foreach (array_keys($firstLine) as $column) {
                    $columns[] = $column;
                }
                $response->setColumns($columns);
            }

            // set rows
            $rows = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row::class);
            foreach ($result->fetchAllAssociative() as $lineNumber => $line) {
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
}
