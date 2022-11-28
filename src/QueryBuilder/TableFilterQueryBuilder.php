<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Storage\Teradata\SelectSource;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand\PreviewTableOrderBy;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use LogicException;

class TableFilterQueryBuilder
{
    public const OPERATOR_SINGLE_VALUE = [
        TableWhereFilter\Operator::eq => '=',
        TableWhereFilter\Operator::ne => '<>',
        TableWhereFilter\Operator::gt => '>',
        TableWhereFilter\Operator::ge => '>=',
        TableWhereFilter\Operator::lt => '<',
        TableWhereFilter\Operator::le => '<=',
    ];
    public const OPERATOR_MULTI_VALUE = [
        TableWhereFilter\Operator::eq => 'IN',
        TableWhereFilter\Operator::ne => 'NOT IN',
    ];

    public const DEFAULT_CAST_SIZE = 16384;

    private Connection $connection;
    private ?TableInfo $tableInfo;
    private TeradataColumnConverter $columnConverter;

    public function __construct(
        Connection $connection,
        ?TableInfo $tableInfo,
        TeradataColumnConverter $columnConverter
    ) {
        $this->connection = $connection;
        $this->tableInfo = $tableInfo;
        $this->columnConverter = $columnConverter;
    }

    public function buildQueryFromCommand(
        PreviewTableCommand $options,
        string $schemaName
    ): TableFilterQueryBuilderResponse {
        $this->assertFilterCombination($options);

        $query = new QueryBuilder($this->connection);

        $this->processChangedConditions($options, $query);

        try {
            if ($options->getFulltextSearch() !== '') {
                if ($this->tableInfo === null) {
                    throw new LogicException('tableInfo variable has to be set to use fulltextSearch');
                }

                $tableInfoColumns = [];
                /** @var TableInfo\TableColumn $column */
                foreach ($this->tableInfo->getColumns() as $column) {
                    // search only in STRING types
                    if ($this->getBasetype($column->getType()) === BaseType::STRING) {
                        $tableInfoColumns[] = $column->getName();
                    }
                }

                $this->buildFulltextFilters(
                    $query,
                    $options->getFulltextSearch(),
                    $tableInfoColumns,
                );
            } else {
                $this->processWhereFilters($options->getWhereFilters(), $query);
            }

            $this->processOrderStatement($options->getOrderBy(), $query);
        } catch (QueryException $e) {
            throw new TableFilterQueryBuilderException(
                $e->getMessage(),
                $e
            );
        }

        $this->processSelectStatement($options, $query);
        $this->processLimitStatement($options, $query);
        $this->processFromStatement($options, $query, $schemaName);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new TableFilterQueryBuilderResponse(
            $sql,
            $query->getParameters(),
            $types,
            ProtobufHelper::repeatedStringToArray($options->getColumns()),
        );
    }

    private function assertFilterCombination(PreviewTableCommand $options): void
    {
        if ($options->getFulltextSearch() !== '' && $options->getWhereFilters()->count()) {
            throw new TableFilterQueryBuilderException(
                'Cannot use fulltextSearch and whereFilters at the same time',
            );
        }
    }

    private function processChangedConditions(PreviewTableCommand $options, QueryBuilder $query): void
    {
        if ($options->getChangeSince() !== '') {
            $query->andWhere('"_timestamp" >= :changedSince');
            $query->setParameter(
                'changedSince',
                (new \DateTime('@' . $options->getChangeSince(), new \DateTimeZone('UTC')))
                    ->format('Y-m-d H:i:s')
            );
        }

        if ($options->getChangeUntil() !== '') {
            $query->andWhere('"_timestamp" < :changedUntil');
            $query->setParameter(
                'changedUntil',
                (new \DateTime('@' . $options->getChangeUntil(), new \DateTimeZone('UTC')))
                    ->format('Y-m-d H:i:s')
            );
        }
    }

    /**
     * @param string[] $columns
     */
    private function buildFulltextFilters(
        QueryBuilder $query,
        string $fulltextSearchKey,
        array $columns
    ): void {
        $platform = $this->connection->getDatabasePlatform();
        assert($platform !== null);
        foreach ($columns as $column) {
            $query->orWhere(
                $query->expr()->like(
                    TeradataQuote::quoteSingleIdentifier($column),
                    $platform->quoteStringLiteral("%{$fulltextSearchKey}%")
                )
            );
        }
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
        if ($value === '') {
            $isAllowedOperator = in_array($filter->getOperator(), [
                TableWhereFilter\Operator::eq,
                TableWhereFilter\Operator::ne,
            ], true);

            if (!$isAllowedOperator) {
                throw new TableFilterQueryBuilderException(
                    'Teradata where filter on empty strings can be used only with "ne, eq" operators.',
                );
            }

            // on empty strings compare null
            $query->andWhere(
                sprintf(
                    '%s %s',
                    TeradataQuote::quoteSingleIdentifier($filter->getColumnsName()),
                    $filter->getOperator() === TableWhereFilter\Operator::eq ? 'IS NULL' : 'IS NOT NULL'
                )
            );
            return;
        }

        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
        } else {
            $columnSql = TeradataQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        if ($filter->getOperator() === TableWhereFilter\Operator::ne) {
            // if not equals add IS NULL condition
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s %s',
                    $columnSql,
                    self::OPERATOR_SINGLE_VALUE[$filter->getOperator()],
                    $query->createNamedParameter($value)
                ),
                sprintf(
                    '%s IS NULL',
                    TeradataQuote::quoteSingleIdentifier($filter->getColumnsName())
                )
            ));
            return;
        }

        // otherwise add normal where
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
        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
        } else {
            $columnSql = TeradataQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        $quotedValues = array_map(static fn(string $value) => TeradataQuote::quote($value), $values);

        if (in_array('', $values, true)) {
            // if empty string is in data we need to compare null
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s (%s)',
                    $columnSql,
                    self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                    implode(',', $quotedValues)
                ),
                sprintf(
                    '%s %s',
                    TeradataQuote::quoteSingleIdentifier($filter->getColumnsName()),
                    $filter->getOperator() === TableWhereFilter\Operator::eq ? 'IS NULL' : 'IS NOT NULL'
                )
            ));
            return;
        }

        if ($filter->getOperator() === TableWhereFilter\Operator::ne) {
            // on not equals we also need to check if value is null
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s (%s)',
                    $columnSql,
                    self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                    implode(',', $quotedValues)
                ),
                sprintf(
                    '%s IS NULL',
                    TeradataQuote::quoteSingleIdentifier($filter->getColumnsName())
                )
            ));
            return;
        }

        $query->andWhere(
            sprintf(
                '%s %s (%s)',
                $columnSql,
                self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                implode(',', $quotedValues)
            )
        );
    }

    private function processOrderStatement(?PreviewTableOrderBy $sort, QueryBuilder $query): void
    {
        if ($sort === null) {
            return;
        }

        if ($sort->getDataType() !== DataType::STRING) {
            $query->addOrderBy(
                $this->columnConverter->convertColumnByDataType($sort->getColumnName(), $sort->getDataType()),
                PreviewTableOrderBy\Order::name($sort->getOrder())
            );
            return;
        }
        $query->addOrderBy(
            TeradataQuote::quoteSingleIdentifier($sort->getColumnName()),
            PreviewTableOrderBy\Order::name($sort->getOrder())
        );
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

    private function processSelectWithLargeColumnTruncation(
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
        //flag if is casted
        $query->addSelect(
            sprintf(
                '(IF LENGTH(%s) > %s THEN 1 ELSE 0 ENDIF) AS %s',
                TeradataQuote::quoteSingleIdentifier($column),
                self::DEFAULT_CAST_SIZE,
                TeradataQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

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

    private function getBasetype(string $type): string
    {
        return (new Teradata($type))->getBasetype();
    }
}
