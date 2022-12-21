<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Info\TableInfo;

class ExportQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(Connection $connection, ?TableInfo $tableInfo): ExportQueryBuilder
    {
        return new ExportQueryBuilder(
            $connection,
            $tableInfo,
            new ColumnConverter(),
        );
    }
}
