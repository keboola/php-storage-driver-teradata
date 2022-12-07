<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Exception;
use Generator;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Teradata\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableExportFilterQueryBuilder;
use Keboola\StorageDriver\Teradata\QueryBuilder\QueryBuilderException;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataPlatform;
use PHPUnit\Framework\TestCase;

class TableExportFilterQueryBuilderTest extends TestCase
{
    /**
     * @dataProvider provideSuccessData
     * @param string[] $expectedBindings
     * @param string[] $expectedDataTypes
     */
    public function testBuildQueryFromCommnand(
        TableExportToFileCommand $exportCommand,
        string $expectedSql,
        array $expectedBindings,
        array $expectedDataTypes
    ): void {
        $connection = $this->createMock(Connection::class);
        $connection->method('getExpressionBuilder')
            ->willReturn(new ExpressionBuilder($this->createMock(Connection::class)));
        $connection->method('getDatabasePlatform')
            ->willReturn(new TeradataPlatform());

        $columnConverter = new ColumnConverter();

        // create query builder
        $qb = new TableExportFilterQueryBuilder($connection, $columnConverter);

        // build query
        $queryData = $qb->buildQueryFromCommand($exportCommand, 'some_schema', 'some_table');

        $this->assertSame(
            str_replace(PHP_EOL, '', $expectedSql),
            $queryData->getQuery(),
        );
        $this->assertSame(
            $expectedBindings,
            $queryData->getBindings(),
        );
        $this->assertSame(
            $expectedDataTypes,
            $queryData->getTypes(),
        );
    }

    public function provideSuccessData(): Generator
    {
        yield 'limit + one filter + orderBy' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'limit' => 100,
                    'columnsToExport' => ['id', 'name'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                    'orderBy' => [
                        new OrderBy([
                            'columnName' => 'name',
                            'order' => Order::ASC,
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
            ]),
            <<<SQL
            SELECT TOP 100 "id", "name" FROM "some_schema"."some_table"
             WHERE "name" <> :dcValue1
             ORDER BY "name" ASC
            SQL,
            [
                'dcValue1' => 'foo',
            ],
            [
                'dcValue1' => ParameterType::STRING,
            ],
        ];
        yield 'more filters + more orderBy' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'columnsToExport' => ['id', 'name'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ge,
                            'values' => ['1.23'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                    'orderBy' => [
                        new OrderBy([
                            'columnName' => 'id',
                            'order' => Order::ASC,
                            'dataType' => DataType::STRING,
                        ]),
                        new OrderBy([
                            'columnName' => 'name',
                            'order' => Order::DESC,
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
            ]),
            <<<SQL
            SELECT "id", "name" FROM "some_schema"."some_table"
             WHERE ("name" <> :dcValue1)
             AND ("height" >= :dcValue2)
             ORDER BY "id" ASC, "name" DESC
            SQL,
            [
                'dcValue1' => 'foo',
                'dcValue2' => '1.23',
            ],
            [
                'dcValue1' => ParameterType::STRING,
                'dcValue2' => ParameterType::STRING,
            ],
        ];
        yield 'changeSince + changeUntil + more columns' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'changeSince' => '1667293200',
                    'changeUntil' => '1669827600',
                    'columnsToExport' => ['id', 'name', 'height', 'birth_at'],
                ]),
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE ("_timestamp" >= :changedSince) AND ("_timestamp" < :changedUntil)
            SQL,
            [
                'changedSince' => '2022-11-01 09:00:00',
                'changedUntil' => '2022-11-30 17:00:00',
            ],
            [
                'changedSince' => ParameterType::STRING,
                'changedUntil' => ParameterType::STRING,
            ],
        ];
        yield 'one filter with type + orderBy with type' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'columnsToExport' => ['id', 'name', 'height', 'birth_at'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ne,
                            'values' => ['10.20'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                    'orderBy' => [
                        new OrderBy([
                            'columnName' => 'id',
                            'order' => Order::ASC,
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE CAST(TO_NUMBER("height") AS REAL) <> :dcValue1
             ORDER BY CAST(TO_NUMBER("id") AS REAL) ASC
            SQL,
            [
                'dcValue1' => '10.20',
            ],
            [
                'dcValue1' => ParameterType::STRING,
            ],
        ];
        yield 'more filters with type' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'columnsToExport' => ['id', 'name', 'height', 'birth_at'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::eq,
                            'values' => ['foo', 'bar'],
                            'dataType' => DataType::STRING,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::ne,
                            'values' => ['50', '60'],
                            'dataType' => DataType::INTEGER,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ne,
                            'values' => ['10.20'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE ("id" IN ('foo','bar'))
             AND (CAST(TO_NUMBER("id") AS INTEGER) NOT IN ('50','60'))
             AND (CAST(TO_NUMBER("height") AS REAL) <> :dcValue1)
            SQL,
            [
                'dcValue1' => '10.20',
            ],
            [
                'dcValue1' => ParameterType::STRING,
            ],
        ];
    }

    /**
     * @dataProvider provideFailedData
     * @param class-string<Exception> $exceptionClass
     */
    public function testBuildQueryFromCommnandFailed(
        TableExportToFileCommand $exportCommand,
        string $exceptionClass,
        string $exceptionMessage
    ): void {
        $connection = $this->createMock(Connection::class);
        $connection->method('getExpressionBuilder')
            ->willReturn(new ExpressionBuilder($this->createMock(Connection::class)));
        $connection->method('getDatabasePlatform')
            ->willReturn(new TeradataPlatform());

        $columnConverter = new ColumnConverter();

        // create query builder
        $qb = new TableExportFilterQueryBuilder($connection, $columnConverter);

        // build query
        $this->expectException($exceptionClass);
        $this->expectExceptionMessage($exceptionMessage);
        $qb->buildQueryFromCommand($exportCommand, 'some_schema', 'table_name');
    }

    public function provideFailedData(): Generator
    {
        yield 'unsupported dataType' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'columnsToExport' => ['id', 'name'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::BIGINT,
                        ]),
                    ],
                ]),
            ]),
            QueryBuilderException::class,
            'Data type BIGINT not recognized. Possible datatypes are [INTEGER|REAL]',
        ];
        yield 'filter with multiple values and GT operator' => [
            new TableExportToFileCommand([
                'exportOptions' => new ExportOptions([
                    'columnsToExport' => ['id', 'name'],
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::gt,
                            'values' => ['foo', 'bar'],
                        ]),
                    ],
                ]),
            ]),
            QueryBuilderException::class,
            'whereFilter with multiple values can be used only with "eq", "ne" operators',
        ];
    }
}
