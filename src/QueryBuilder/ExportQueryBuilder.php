<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use LogicException;

class ExportQueryBuilder extends CommonFilterQueryBuilder
{
    public function __construct(
        Connection $connection,
        ColumnConverter $columnConverter
    ) {
        parent::__construct($connection, $columnConverter);
    }

    public function buildQueryFromCommand(
        ?ExportFilters $filters,
        RepeatedField $orderBy,
        RepeatedField $columnsToUse,
        ColumnCollection $tableColumnsDefinitions,
        string $schemaName,
        string $tableName,
        bool $truncateLargeColumns
    ): QueryBuilderResponse {

        $query = new QueryBuilder($this->connection);

        if ($filters !== null) {
            $this->assertFilterCombination($filters);
            $this->processFilters($filters, $query, $tableColumnsDefinitions);
        }

        $this->processOrderStatement($orderBy, $query);
        $this->processSelectStatement(
            ProtobufHelper::repeatedStringToArray($columnsToUse),
            $query,
            $tableColumnsDefinitions,
            $truncateLargeColumns
        );
        $this->processFromStatement($schemaName, $tableName, $query);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new QueryBuilderResponse(
            $sql,
            $query->getParameters(),
            $types,
            ProtobufHelper::repeatedStringToArray($columnsToUse),
        );
    }

    private function assertFilterCombination(ExportFilters $options): void
    {
        if ($options->getFulltextSearch() !== '' && $options->getWhereFilters()->count()) {
            throw new QueryBuilderException(
                'Cannot use fulltextSearch and whereFilters at the same time',
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
        foreach ($columns as $column) {
            $query->orWhere(
                $query->expr()->like(
                    TeradataQuote::quoteSingleIdentifier($column),
                    TeradataQuote::quote("%{$fulltextSearchKey}%")
                )
            );
        }
    }

    private function getBasetype(string $type): string
    {
        return (new Teradata($type))->getBasetype();
    }

    private function processFilters(ExportFilters $filters, QueryBuilder $query, ColumnCollection $tableColumnsDefinitions): void
    {
        $this->processChangedConditions($filters->getChangeSince(), $filters->getChangeUntil(), $query);

        try {
            if ($filters->getFulltextSearch() !== '') {


                $tableInfoColumns = [];
                /** @var TeradataColumn $column */
                foreach ($tableColumnsDefinitions as $column) {
                    // search only in STRING types
                    if ($this->getBasetype($column->getColumnDefinition()->getType()) === BaseType::STRING) {
                        $tableInfoColumns[] = $column->getColumnName();
                    }
                }

                $this->buildFulltextFilters(
                    $query,
                    $filters->getFulltextSearch(),
                    $tableInfoColumns,
                );
            } else {
                $this->processWhereFilters($filters->getWhereFilters(), $query);
            }

        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }
        $this->processLimitStatement($filters->getLimit(), $query);
    }


    /**
     * @param list<mixed>|array<string, mixed> $bindings
     * @param array<string, string|int> $types
     */
    public function replaceNamedParametersWithValues(string $sql, array $bindings, array $types): string
    {
        foreach ($bindings as $name => $value) {
            assert(is_string($name));
            assert(is_string($value) || is_numeric($value));
            // check type
            $type = $types[$name] ?? 'unknown';
            if ($type !== ParameterType::STRING) {
                throw new LogicException(sprintf(
                    'Error while process SQL with bindings: type %s not supported',
                    $type,
                ));
            }

            $count = 0;
            $sql = preg_replace(
                sprintf('/:%s\b/', preg_quote((string) $name, '/')),
                TeradataQuote::quote((string) $value),
                $sql,
                -1,
                $count,
            );
            assert(is_string($sql));

            if ($count === 0) {
                throw new LogicException(sprintf(
                    'Error while process SQL with bindings: binding %s not found',
                    $name,
                ));
            }
        }

        return $sql;
    }
}
