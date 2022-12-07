<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
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
}
