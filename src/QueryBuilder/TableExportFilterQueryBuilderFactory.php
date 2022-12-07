<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;

class TableExportFilterQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(Connection $connection): TableExportFilterQueryBuilder
    {
        return new TableExportFilterQueryBuilder(
            $connection,
            new ColumnConverter(),
        );
    }
}
