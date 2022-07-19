<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Import;

use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand\SourceTableMapping\ColumnMapping;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;

final class StageTableDefinitionFactory
{
    private const VARCHAR_MAX = '32000';
    private const VARCHAR_MAX_TPT = '10666';

    /**
     * @param ColumnMapping[] $columnsMapping
     */
    public static function createStagingTableDefinitionWithMapping(
        TeradataTableDefinition $destination,
        array $columnsMapping
    ): TeradataTableDefinition {
        $newDefinitions = [];
        $primaries = $destination->getPrimaryKeysNames();
        foreach ($columnsMapping as $columnMapping) {
            /** @var TeradataColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnMapping->getDestinationColumnName()) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new TeradataColumn(
                        $columnMapping->getDestinationColumnName(),
                        new Teradata(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => !in_array($columnMapping->getDestinationColumnName(), $primaries),
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ]
                        )
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumn($columnMapping->getDestinationColumnName());
        }

        return new TeradataTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames()
        );
    }

    private static function createVarcharColumn(string $columnName): TeradataColumn
    {
        return new TeradataColumn(
            $columnName,
            new Teradata(
                Teradata::TYPE_VARCHAR,
                [
                    'length' => self::VARCHAR_MAX,
                    'nullable' => true,
                ]
            )
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinitionForTPT(
        TeradataTableDefinition $destination,
        array $sourceColumnsNames
    ): TeradataTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var TeradataColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    $length = $definition->getColumnDefinition()->getLength();
                    $isVarchar = $definition->getColumnDefinition()->getType() === Teradata::TYPE_VARCHAR;
                    if ($isVarchar && (int) $length > self::VARCHAR_MAX_TPT) {
                        // there is nonsense limit in Teradata for TPT to load maximal 10666 characters
                        $length = self::VARCHAR_MAX_TPT;
                    }
                    // if column exists in destination set destination type
                    $newDefinitions[] = new TeradataColumn(
                        $columnName,
                        new Teradata(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $length,
                                'nullable' => true,
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ]
                        )
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumnForTPT($columnName);
        }

        return new TeradataTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            [] // <-- ignore primary keys
        );
    }

    private static function createVarcharColumnForTPT(string $columnName): TeradataColumn
    {
        return new TeradataColumn(
            $columnName,
            new Teradata(
                Teradata::TYPE_VARCHAR,
                [
                    'length' => self::VARCHAR_MAX_TPT,
                    'nullable' => true,
                ]
            )
        );
    }
}
