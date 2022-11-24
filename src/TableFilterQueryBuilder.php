<?php

declare(strict_types=1);

namespace Keboola\Package\StorageBackend\ImportExport\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Keboola\Db\ImportExport\Storage\Exasol\SelectSource;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\Package\Bridge\StorageBackend\ImportExport\QueryBuilder\MetadataColumns;
use Keboola\Package\StorageBackend\ColumnConverter\ExasolColumnConverter;
use Keboola\Package\StorageBackend\ImportExport\ExportWhereFilter;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Storage_Service_BucketBackend_TableExportOptions as TableExportOptions;

/**
 * There is a issue with Doctrine with binding string arrays
 * Prepare will throw array string conversion errors as doctrine will pass it as `[["PRG","VAN"]]`
 * to overcome this array values are passed and escaped manually this could possibly lead to SQL injection
 */
final class ExasolExportQueryBuilder implements ExportQueryBuilderInterface
{
    private MetadataColumns $metadataColumns;

    private Connection $connection;

    private ExasolColumnConverter $columnConverter;

    public function __construct(
        Connection $connection,
        MetadataColumns $metadataColumns,
        ExasolColumnConverter $columnConverter
    ) {
        $this->metadataColumns = $metadataColumns;
        $this->connection = $connection;
        $this->columnConverter = $columnConverter;
    }

    /**
     * @return SelectSource
     */
    public function buildQueryFromOptions(
        TableExportOptions $options,
        string $schemaName,
        int $bucketId
    ): SqlSourceInterface {
        $this->assertFilterCombination($options);
        $query = new QueryBuilder($this->connection);
        $this->processChangedConditions($options, $query);
        try {
            if ($options->getFulltextSearchKey() !== null) {
                $this->buildFulltextFilters(
                    $query,
                    $options->getFulltextSearchKey(),
                    $this->metadataColumns->getColumnNamesForTableInBucket($bucketId, $options->getTableName())
                );
            } else {
                $this->processWhereFilters($options, $query);
            }
            $this->processOrderStatement($options, $query);
        } catch (QueryException $e) {
            throw new ExportQueryBuilderException(
                $e->getMessage(),
                ExportQueryBuilderException::STRING_CODE_TABLE_VALIDATION,
                $e
            );
        }
        $this->processSelectStatement($options, $query);
        $this->processLimitStatement($options, $query);
        $this->processFromStatement($options, $query, $schemaName);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new SelectSource(
            $sql,
            $query->getParameters(),
            $types,
            $options->getColumns()
        );
    }

    private function assertFilterCombination(TableExportOptions $options): void
    {
        if ($options->getFulltextSearchKey() && count($options->getWhereFilters())) {
            throw new ExportQueryBuilderException(
                'Cannot use fulltextSearch and whereFilters at the same time',
                ExportQueryBuilderException::STRING_CODE_TABLE_VALIDATION
            );
        }
    }

    private function processChangedConditions(TableExportOptions $options, QueryBuilder $query): void
    {
        if ($options->getChangedSince() !== null) {
            $query->andWhere('"_timestamp" >= :changedSince');
            $query->setParameter('changedSince', (new \DateTime('@' . $options->getChangedSince(), new \DateTimeZone('UTC')))->format('Y-m-d H:i:s'));
        }

        if ($options->getChangedUntil() !== null) {
            $query->andWhere('"_timestamp" < :changedUntil');
            $query->setParameter('changedUntil', (new \DateTime('@' . $options->getChangedUntil(), new \DateTimeZone('UTC')))->format('Y-m-d H:i:s'));
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
        foreach ($columns as $column) {
            $query->orWhere($query->expr()->like(ExasolQuote::quoteSingleIdentifier($column), $query->expr()->literal("%{$fulltextSearchKey}%")));
        }
    }

    private function processWhereFilters(TableExportOptions $options, QueryBuilder $query): void
    {
        foreach ($options->getWhereFilters() as $whereFilter) {
            $values = $whereFilter->getValues();
            if (count($values) === 1) {
                $this->processSimpleValue($whereFilter, reset($values), $query);
            } else {
                $this->processMultipleValue($whereFilter, $values, $query);
            }
        }
    }

    private function processSimpleValue(ExportWhereFilter $filter, string $value, QueryBuilder $query): void
    {
        if ($value === '') {
            $isAllowedOperator = in_array($filter->getOperator(), [
                ExportWhereFilter::OPERATOR_EQ,
                ExportWhereFilter::OPERATOR_NE,
            ], true);

            if (!$isAllowedOperator) {
                throw new ExportQueryBuilderException(
                    'Exasol where filter on empty strings can be used only with "ne, eq" operators.',
                    ExportQueryBuilderException::STRING_CODE_TABLE_VALIDATION
                );
            }

            // on empty strings compare null
            $query->andWhere(
                sprintf(
                    '%s %s',
                    ExasolQuote::quoteSingleIdentifier($filter->getColumn()),
                    $filter->getOperator() === ExportWhereFilter::OPERATOR_EQ ? 'IS NULL' : 'IS NOT NULL'
                )
            );
            return;
        }

        if ($filter->getDataType() !== null) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumn(),
                $filter->getDataType()
            );
        } else {
            $columnSql = ExasolQuote::quoteSingleIdentifier($filter->getColumn());
        }

        if ($filter->getOperator() === ExportWhereFilter::OPERATOR_NE) {
            // if not equals add IS NULL condition
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s %s',
                    $columnSql,
                    ExportWhereFilter::OPERATOR_MAP_SINGLE_VALUE[$filter->getOperator()],
                    $query->createNamedParameter($value)
                ),
                sprintf(
                    '%s IS NULL',
                    ExasolQuote::quoteSingleIdentifier($filter->getColumn())
                )
            ));
            return;
        }

        // otherwise add normal where
        $query->andWhere(
            sprintf(
                '%s %s %s',
                $columnSql,
                ExportWhereFilter::OPERATOR_MAP_SINGLE_VALUE[$filter->getOperator()],
                $query->createNamedParameter($value)
            )
        );
    }

    /**
     * @param array<mixed> $values
     */
    private function processMultipleValue(ExportWhereFilter $filter, array $values, QueryBuilder $query): void
    {
        if ($filter->getDataType() !== null) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumn(),
                $filter->getDataType()
            );
        } else {
            $columnSql = ExasolQuote::quoteSingleIdentifier($filter->getColumn());
        }

        $quotedValues = array_map(static fn(string $value) => ExasolQuote::quote($value), $values);
        if (in_array('', $values, true)) {
            // if empty string is in data we need to compare null
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s (%s)',
                    $columnSql,
                    ExportWhereFilter::OPERATOR_MAP_MULTI_VALUE[$filter->getOperator()],
                    implode(',', $quotedValues)
                ),
                sprintf(
                    '%s %s',
                    ExasolQuote::quoteSingleIdentifier($filter->getColumn()),
                    $filter->getOperator() === ExportWhereFilter::OPERATOR_EQ ? 'IS NULL' : 'IS NOT NULL'
                )
            ));
            return;
        }

        if ($filter->getOperator() === ExportWhereFilter::OPERATOR_NE) {
            // on not equals we also need to check if value is null
            $query->andWhere($query->expr()->or(
                sprintf(
                    '%s %s (%s)',
                    $columnSql,
                    ExportWhereFilter::OPERATOR_MAP_MULTI_VALUE[$filter->getOperator()],
                    implode(',', $quotedValues)
                ),
                sprintf(
                    '%s IS NULL',
                    ExasolQuote::quoteSingleIdentifier($filter->getColumn())
                )
            ));
            return;
        }
        $quotedValues = array_map(static fn(string $value) => ExasolQuote::quote($value), $values);
        $query->andWhere(
            sprintf(
                '%s %s (%s)',
                $columnSql,
                ExportWhereFilter::OPERATOR_MAP_MULTI_VALUE[$filter->getOperator()],
                implode(',', $quotedValues)
            )
        );
    }

    private function processOrderStatement(TableExportOptions $options, QueryBuilder $query): void
    {
        foreach ($options->getOrderByStatements() as $sort) {
            if ($sort->getDataType() !== null) {
                $query->addOrderBy(
                    $this->columnConverter->convertColumnByDataType($sort->getColumn(), $sort->getDataType()),
                    $sort->getOrder()
                );
            } else {
                $query->addOrderBy(ExasolQuote::quoteSingleIdentifier($sort->getColumn()), $sort->getOrder());
            }
        }
    }

    private function processSelectStatement(TableExportOptions $options, QueryBuilder $query): void
    {
        foreach ($options->getColumns() as $column) {
            $selectColumnExpresion = ExasolQuote::quoteSingleIdentifier($column);

            if ($options->shouldTruncateLargeColumns()) {
                $this->processSelectWithLargeColumnTruncation($query, $selectColumnExpresion, $options, $column);
            } else {
                $query->addSelect($selectColumnExpresion);
            }
        }
    }

    private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumnExpresion,
        TableExportOptions $options,
        string $column
    ): void {
        //casted value
        $query->addSelect(
            sprintf(
                'CAST(SUBSTRING(%s, 0, %d) as VARCHAR(%d)) AS %s',
                $selectColumnExpresion,
                $options->getCastSize(),
                $options->getCastSize(),
                ExasolQuote::quoteSingleIdentifier($column)
            )
        );
        //flag if is casted
        $query->addSelect(
            sprintf(
                '(IF LENGTH(%s) > %s THEN 1 ELSE 0 ENDIF) AS %s',
                ExasolQuote::quoteSingleIdentifier($column),
                $options->getCastSize(),
                ExasolQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

    private function processLimitStatement(TableExportOptions $options, QueryBuilder $query): void
    {
        if ($options->getLimit() !== null) {
            $query->setMaxResults((int) $options->getLimit());
        }
    }

    private function processFromStatement(TableExportOptions $options, QueryBuilder $query, string $schemaName): void
    {
        $query->from(sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($options->getTableName())
        ));
    }
}
