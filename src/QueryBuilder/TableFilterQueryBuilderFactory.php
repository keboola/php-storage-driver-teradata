<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Info\TableInfo;

class TableFilterQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(Connection $connection, ?TableInfo $tableInfo): TableFilterQueryBuilder
    {
        return new TableFilterQueryBuilder(
            $connection,
            $tableInfo,
            new ColumnConverter(),
        );
    }
}
