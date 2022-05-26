<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Info\TableReflection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;

class TableReflectionResponseTransformer
{
    public static function transformTableReflectionToResponse(
        string $database,
        TableReflectionInterface $ref
    ): TableReflection {
        $res = new TableReflection();
        $def = $ref->getTableDefinition();
        $columns = new RepeatedField(GPBType::MESSAGE, TableReflection\TableColumn::class);
        /** @var TeradataColumn $col */
        foreach ($def->getColumnsDefinitions() as $col) {
            /** @var Teradata $colDef */
            $colDef = $col->getColumnDefinition();

            $colInternal = (new TableReflection\TableColumn())
                ->setName($col->getColumnName())
                ->setType($colDef->getType())
                ->setNullable($colDef->isNullable());

            if ($colDef->getLength() !== null) {
                $colInternal->setLength($colDef->getLength());
            }

            if ($colDef->getDefault() !== null) {
                $colInternal->setDefault($colDef->getDefault());
            }

            $columns[] = $colInternal;
        }
        $res->setColumns($columns);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $database;
        $res->setPath($path);
        $res->setTableName($def->getTableName());
        $pk = new RepeatedField(GPBType::STRING);
        foreach ($def->getPrimaryKeysNames() as $col) {
            $pk[] = $col;
        }
        $res->setPrimaryKeysNames($pk);

        return $res;
    }
}
