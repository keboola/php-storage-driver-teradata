<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;

class ExportQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(Connection $connection): ExportQueryBuilder
    {
        return new ExportQueryBuilder(
            $connection,
            new ColumnConverter(),
        );
    }
}
