<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Doctrine\DBAL\Connection;
use Generator;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

abstract class ImportBaseCase extends BaseCase
{

    protected function createDestinationTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        Connection $db
    ): TeradataTableDefinition {
        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
                TeradataColumn::createGenericColumn('_timestamp'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);
        // init some values
        foreach ([['1', '2', '4', ''], ['2', '3', '4', ''], ['3', '3', '3', '']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $i)
            ));
        }
        return $tableDestDef;
    }


    protected function createDestinationTypedTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        Connection $db
    ): TeradataTableDefinition {
        $tableDestDef = new TeradataTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new TeradataColumn('col1', new Teradata(
                    Teradata::TYPE_INT,
                    []
                )),
                new TeradataColumn('col2', new Teradata(
                    Teradata::TYPE_BIGINT,
                    []
                )),
                TeradataColumn::createGenericColumn('col3'),
                TeradataColumn::createGenericColumn('_timestamp'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);
        // init some values
        foreach ([[1, 2, '4', ''], [2, 3, '4', ''], [3, 3, '3', '']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $i)
            ));
        }
        return $tableDestDef;
    }


    /**
     * @return Generator<string,array{boolean}>
     */
    public function isTypedTablesProvider(): Generator
    {
        yield 'typed ' => [true];
        yield 'string table ' => [false];
    }
}
