<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

abstract class CommonFilterQueryBuilder
{
    public const DEFAULT_CAST_SIZE = 16384;
    private const IEEE_TYPES = [
        Teradata::TYPE_FLOAT,
        Teradata::TYPE_DOUBLE_PRECISION,
        Teradata::TYPE_REAL,
    ];
    public const OPERATOR_SINGLE_VALUE = [
        Operator::eq => '=',
        Operator::ne => '<>',
        Operator::gt => '>',
        Operator::ge => '>=',
        Operator::lt => '<',
        Operator::le => '<=',
    ];
    public const OPERATOR_MULTI_VALUE = [
        Operator::eq => 'IN',
        Operator::ne => 'NOT IN',
    ];

    protected Connection $connection;

    protected ColumnConverter $columnConverter;

    public function __construct(
        Connection $connection,
        ColumnConverter $columnConverter
    ) {
        $this->connection = $connection;
        $this->columnConverter = $columnConverter;
    }

    private function addSelectLargeString(QueryBuilder $query, string $selectColumnExpresion, string $column): void
    {
        //casted value
        $query->addSelect(
            sprintf(
                'CAST(SUBSTRING(CAST(%s as VARCHAR(%d)), 0, %d) as VARCHAR(%d)) AS %s',
                $selectColumnExpresion,
                self::DEFAULT_CAST_SIZE,
                self::DEFAULT_CAST_SIZE,
                self::DEFAULT_CAST_SIZE,
                TeradataQuote::quoteSingleIdentifier($column)
            )
        );
        //flag if is cast
        $query->addSelect(
            sprintf(
                '(CASE WHEN LENGTH(CAST(%s as VARCHAR(%d))) > %s THEN 1 ELSE 0 END) AS %s',
                TeradataQuote::quoteSingleIdentifier($column),
                self::DEFAULT_CAST_SIZE + 1, // cast to one extra character to recognize difference
                self::DEFAULT_CAST_SIZE,
                TeradataQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

    protected function processChangedConditions(string $changeSince, string $changeUntil, QueryBuilder $query): void
    {
        if ($changeSince !== '') {
            $query->andWhere('"_timestamp" >= :changedSince');
            $query->setParameter(
                'changedSince',
                $this->getTimestampFormatted($changeSince),
            );
        }

        if ($changeUntil !== '') {
            $query->andWhere('"_timestamp" < :changedUntil');
            $query->setParameter(
                'changedUntil',
                $this->getTimestampFormatted($changeUntil),
            );
        }
    }

    /**
     * @param string $timestamp
     */
    private function getTimestampFormatted(string $timestamp): string
    {
        return (new DateTime('@' . $timestamp, new DateTimeZone('UTC')))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param RepeatedField|TableWhereFilter[] $filters
     */
    protected function processWhereFilters(RepeatedField $filters, QueryBuilder $query): void
    {
        foreach ($filters as $whereFilter) {
            $values = ProtobufHelper::repeatedStringToArray($whereFilter->getValues());
            if (count($values) === 1) {
                $this->processSimpleValue($whereFilter, reset($values), $query);
            } else {
                $this->processMultipleValue($whereFilter, $values, $query);
            }
        }
    }

    private function processSimpleValue(TableWhereFilter $filter, string $value, QueryBuilder $query): void
    {
        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
        } else {
            $columnSql = TeradataQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        $query->andWhere(
            sprintf(
                '%s %s %s',
                $columnSql,
                self::OPERATOR_SINGLE_VALUE[$filter->getOperator()],
                $query->createNamedParameter($value),
            )
        );
    }

    /**
     * @param string[] $values
     */
    private function processMultipleValue(TableWhereFilter $filter, array $values, QueryBuilder $query): void
    {
        if (!array_key_exists($filter->getOperator(), self::OPERATOR_MULTI_VALUE)) {
            throw new QueryBuilderException(
                'whereFilter with multiple values can be used only with "eq", "ne" operators',
            );
        }

        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
        } else {
            $columnSql = TeradataQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        $quotedValues = array_map(static fn(string $value) => TeradataQuote::quote($value), $values);

        $query->andWhere(
            sprintf(
                '%s %s (%s)',
                $columnSql,
                self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                implode(',', $quotedValues)
            )
        );
    }

    /**
     * @param RepeatedField|ExportOrderBy[] $sort
     */
    protected function processOrderStatement(RepeatedField $sort, QueryBuilder $query): void
    {
        try {
            foreach ($sort as $orderBy) {
                if ($orderBy->getDataType() !== DataType::STRING) {
                    $query->addOrderBy(
                        $this->columnConverter->convertColumnByDataType($orderBy->getColumnName(), $orderBy->getDataType()),
                        Order::name($orderBy->getOrder())
                    );
                    return;
                }
                $query->addOrderBy(
                    TeradataQuote::quoteSingleIdentifier($orderBy->getColumnName()),
                    Order::name($orderBy->getOrder())
                );
            }
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }
    }

    /**
     * @param string[] $columns
     */
    protected function processSelectStatement(
        array $columns,
        QueryBuilder $query,
        ColumnCollection $tableColumnsDefinitions,
        bool $truncateLargeColumns
    ): void {
        if (count($columns) === 0) {
            $query->addSelect('*');
            return;
        }

        foreach ($columns as $column) {
            $selectColumnExpresion = TeradataQuote::quoteSingleIdentifier($column);

            if ($truncateLargeColumns) {
                /** @var TeradataColumn[] $defs */
                $defs = iterator_to_array($tableColumnsDefinitions);
                /** @var TeradataColumn $def */
                $def = array_values(array_filter(
                    $defs,
                    fn(TeradataColumn $c) => $c->getColumnName() === $column
                ))[0];
                $this->processSelectWithLargeColumnTruncation(
                    $query,
                    $selectColumnExpresion,
                    $column,
                    $def->getColumnDefinition()
                );
                continue;
            }
            $query->addSelect($selectColumnExpresion);
        }
    }

    private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumnExpresion,
        string $column,
        Teradata $def
    ): void {
        if ($def->getBasetype() === BaseType::STRING) {
            $this->addSelectLargeString($query, $selectColumnExpresion, $column);
            return;
        }
        if (in_array($def->getType(), self::IEEE_TYPES, true)) {
            // dont cast IEEE-754 types as they would be exported in scientific notation
            $query->addSelect(
                sprintf(
                    '%s AS %s',
                    $selectColumnExpresion,
                    TeradataQuote::quoteSingleIdentifier($column)
                )
            );
        } else {
            //cast value to string
            $query->addSelect(
                sprintf(
                    'CAST(%s as VARCHAR(%d)) AS %s',
                    $selectColumnExpresion,
                    self::DEFAULT_CAST_SIZE,
                    TeradataQuote::quoteSingleIdentifier($column)
                )
            );
        }

        //flag if is cast
        $query->addSelect(
            sprintf(
                '\'0\' AS %s',
                TeradataQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

    protected function processLimitStatement(int $limit, QueryBuilder $query): void
    {
        if ($limit > 0) {
            $query->setMaxResults($limit);
        }
    }
}
