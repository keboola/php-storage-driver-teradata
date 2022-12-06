<?php

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Info\TableInfo;

class TablePreviewFilterQueryBuilderFactory
{
    public function __construct()
    {
    }

    public function create(Connection $connection, ?TableInfo $tableInfo): TablePreviewFilterQueryBuilder
    {
        return new TablePreviewFilterQueryBuilder(
            $connection,
            $tableInfo,
            new ColumnConverter(),
        );
    }
}
