<?php

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumnConverter;

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
            new TeradataColumnConverter(),
        );
    }
}
