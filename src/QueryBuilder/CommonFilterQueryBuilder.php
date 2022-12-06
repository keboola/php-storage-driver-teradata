<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

abstract class CommonFilterQueryBuilder
{
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

    private function processChangedConditions(PreviewTableCommand $options, QueryBuilder $query): void
    {
        if ($options->getChangeSince() !== '') {
            $query->andWhere('"_timestamp" >= :changedSince');
            $query->setParameter(
                'changedSince',
                $this->getTimestampFormatted($options->getChangeSince()),
            );
        }

        if ($options->getChangeUntil() !== '') {
            $query->andWhere('"_timestamp" < :changedUntil');
            $query->setParameter(
                'changedUntil',
                $this->getTimestampFormatted($options->getChangeUntil()),
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
    private function processWhereFilters(RepeatedField $filters, QueryBuilder $query): void
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
                $query->createNamedParameter($value)
            )
        );
    }

    /**
     * @param string[] $values
     */
    private function processMultipleValue(TableWhereFilter $filter, array $values, QueryBuilder $query): void
    {
        if (!array_key_exists($filter->getOperator(), self::OPERATOR_MULTI_VALUE)) {
            throw new TableFilterQueryBuilderException(
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
     * @param RepeatedField|OrderBy[] $sort
     */
    private function processOrderStatement(RepeatedField $sort, QueryBuilder $query): void
    {
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
    }

    private function processSelectStatement(PreviewTableCommand $options, QueryBuilder $query): void
    {
        /** @var string $column */
        foreach ($options->getColumns() as $column) {
            $selectColumnExpresion = TeradataQuote::quoteSingleIdentifier($column);

            // TODO truncate - preview does not contains export format
            //if ($options->shouldTruncateLargeColumns()) {
            //    $this->processSelectWithLargeColumnTruncation($query, $selectColumnExpresion, $column);
            //    return;
            //}
            $query->addSelect($selectColumnExpresion);
        }
    }

    // TODO truncate - preview does not contains export format
    /*private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumnExpresion,
        string $column
    ): void {
        //casted value
        $query->addSelect(
            sprintf(
                'CAST(SUBSTRING(%s, 0, %d) as VARCHAR(%d)) AS %s',
                $selectColumnExpresion,
                self::DEFAULT_CAST_SIZE,
                self::DEFAULT_CAST_SIZE,
                TeradataQuote::quoteSingleIdentifier($column)
            )
        );
        //flag if is cast
        $query->addSelect(
            sprintf(
                '(IF LENGTH(%s) > %s THEN 1 ELSE 0 ENDIF) AS %s',
                TeradataQuote::quoteSingleIdentifier($column),
                self::DEFAULT_CAST_SIZE,
                TeradataQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }*/

    private function processLimitStatement(PreviewTableCommand $options, QueryBuilder $query): void
    {
        if ($options->getLimit() !== 0) {
            $query->setMaxResults($options->getLimit());
        }
    }

    private function processFromStatement(PreviewTableCommand $options, QueryBuilder $query, string $schemaName): void
    {
        $query->from(sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($options->getTableName())
        ));
    }
}
