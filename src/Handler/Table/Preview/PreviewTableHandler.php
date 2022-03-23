<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    public const STRING_MAX_LENGTH = 50;

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
        assert($command->getPath()->count() === 1, 'CreateTableCommand.path is required and size must equal 1');
        assert(!empty($command->getTableName()), 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        try {
            $db = $this->manager->createSession($credentials);
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];

            // build sql
            $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
            // TODO changeSince, changeUntil
            // TODO fulltextSearch
            // TODO whereFilters
            $selectTableSql = sprintf(
                "SELECT %s %s\nFROM %s.%s",
                $command->getLimit() ? sprintf('TOP %d', $command->getLimit()) : '',
                implode(', ', array_map([TeradataQuote::class, 'quoteSingleIdentifier'], $columns)),
                TeradataQuote::quoteSingleIdentifier($databaseName),
                TeradataQuote::quoteSingleIdentifier($command->getTableName())
            );

            if ($command->hasOrderBy() && $command->getOrderBy()) {
                $orderBy = $command->getOrderBy();
                $selectTableSql .= sprintf(
                    "\nORDER BY %s %s",
                    // TODO k cemu je potreba dataType?
                    TeradataQuote::quoteSingleIdentifier($orderBy->getColumnName()),
                    $orderBy->getOrder() === PreviewTableCommand\PreviewTableOrderBy\Order::ASC ? 'ASC' : 'DESC'
                );
            }

            // select table
            $result = $db->executeQuery($selectTableSql);

            // get columns definitions
            $tableRef = new TeradataTableReflection($db, $databaseName, $command->getTableName());
            /** @var TeradataColumn[] $columnsDefArr */
            $columnsDefArr = iterator_to_array($tableRef->getColumnsDefinitions());
            /** @var array<string, Teradata> $columnsDef */
            $columnsDef = [];
            foreach ($columnsDefArr as $index => $columnDef) {
                $columnsDef[$columnDef->getColumnName()] = $columnDef->getColumnDefinition();
            }

            // set response
            $response = new PreviewTableResponse();

            $rows = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row::class);
            foreach ($result->fetchAllAssociative() as $lineNumber => $line) {
                // set column names
                if ($lineNumber === 0) {
                    $columns = new RepeatedField(GPBType::STRING);
                    foreach (array_keys($line) as $column) {
                        $columns[] = $column;
                    }
                    $response->setColumns($columns);
                }

                // set row
                $row = new PreviewTableResponse\Row();
                $columns = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row\Column::class);
                foreach ($line as $itemKey => $itemValue) {
                    // set row columns
                    $value = new Value();
                    $truncated = false;
                    if ($itemValue === null) {
                        $value->setNullValue(NullValue::NULL_VALUE);
                    } else {
                        // preview returns all data as string
                        switch ($columnsDef[$itemKey]->getBasetype()) {
                            case BaseType::NUMERIC:
                            case BaseType::INTEGER:
                            case BaseType::FLOAT:
                            case BaseType::TIMESTAMP:
                            case BaseType::DATE:
                            case BaseType::STRING:
                                if (mb_strlen((string) $itemValue) > self::STRING_MAX_LENGTH) {
                                    $truncated = true;
                                    $value->setStringValue(mb_substr((string) $itemValue, 0, self::STRING_MAX_LENGTH));
                                } else {
                                    $value->setStringValue((string) $itemValue);
                                }
                                break;
                        }
                    }

                    $columns[] = (new PreviewTableResponse\Row\Column())
                        ->setColumnName($itemKey)
                        ->setValue($value)
                        ->setIsTruncated($truncated);

                    $row->setColumns($columns);
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
}
