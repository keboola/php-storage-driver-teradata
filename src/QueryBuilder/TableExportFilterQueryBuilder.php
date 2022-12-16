<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use LogicException;

class TableExportFilterQueryBuilder extends CommonFilterQueryBuilder
{
    public function buildQueryFromCommand(
        TableExportToFileCommand $command,
        string $schemaName,
        string $tableName
    ): QueryBuilderResponse {
        $options = $command->getExportOptions() ?? new ExportOptions();

        $query = new QueryBuilder($this->connection);

        $this->processChangedConditions($options->getChangeSince(), $options->getChangeUntil(), $query);

        try {
            $this->processWhereFilters($options->getWhereFilters(), $query);

            $this->processOrderStatement($options->getOrderBy(), $query);
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }

        $this->processSelectStatement(ProtobufHelper::repeatedStringToArray($options->getColumnsToExport()), $query);
        $this->processLimitStatement($options->getLimit(), $query);
        $this->processFromStatement($schemaName, $tableName, $query);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new QueryBuilderResponse(
            $sql,
            $query->getParameters(),
            $types,
            ProtobufHelper::repeatedStringToArray($options->getColumnsToExport()),
        );
    }

    /**
     * @param list<mixed>|array<string, mixed> $bindings
     * @param array<string, string|int> $types
     */
    public static function processSqlWithBindingParameters(string $sql, array $bindings, array $types): string
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
                    'Errow while process SQL with bindings: binding %s not found',
                    $name,
                ));
            }
        }

        return $sql;
    }
}
