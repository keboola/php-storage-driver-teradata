<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Exception;
use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\Teradata\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Teradata\QueryBuilder\QueryBuilderException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataPlatform;
use PHPUnit\Framework\TestCase;

class ExportQueryBuilderTest extends TestCase
{
    /**
     * @dataProvider provideSuccessData
     * @param string[] $expectedBindings
     * @param string[] $expectedDataTypes
     */
    public function testBuildQueryFromCommnand(
        PreviewTableCommand $previewCommand,
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

        // define table info
        $tableInfoColumns = [];
        $tableInfoColumns[] = new TeradataColumn('id', new Teradata('INT', [
            'length' => '',
            'nullable' => false,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('name', new Teradata('VARCHAR', [
            'length' => '100',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('height', new Teradata('DECIMAL', [
            'length' => '4,2',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('birth_at', new Teradata('DATE', [
            'length' => '',
            'nullable' => true,
            'default' => '',
        ]));

        // create query builder
        $qb = new ExportQueryBuilder($connection, $columnConverter);

        // build query
        $queryData = $qb->buildQueryFromCommand(
            $previewCommand->getFilters(),
            $previewCommand->getOrderBy(),
            $previewCommand->getColumns(),
            new ColumnCollection($tableInfoColumns),
            'some_schema',
            'some_table',
            false
        );

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
        yield 'empty columns' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
            ]),
            <<<SQL
            SELECT * FROM "some_schema"."some_table"
            SQL,
            [],
            [],
        ];
        yield 'limit + one filter + orderBy' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 100,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'name',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                ],
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
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
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
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                    new ExportOrderBy([
                        'columnName' => 'name',
                        'order' => ExportOrderBy\Order::DESC,
                        'dataType' => DataType::STRING,
                    ]),
                ],
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
        yield 'search + more columns' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => 'foo',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE "name" LIKE '%foo%'
            SQL,
            [],
            [],
        ];
        yield 'changeSince + changeUntil' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '1667293200',
                    'changeUntil' => '1669827600',

                    'fulltextSearch' => '',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            <<<SQL
            SELECT "id", "name" FROM "some_schema"."some_table"
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
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ne,
                            'values' => ['10.20'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::REAL,
                    ]),
                ],
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
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'tableName' => 'some_table',
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
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
                'orderBy' => [],
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
        PreviewTableCommand $previewCommand,
        string $exceptionClass,
        string $exceptionMessage
    ): void {
        $connection = $this->createMock(Connection::class);
        $connection->method('getExpressionBuilder')
            ->willReturn(new ExpressionBuilder($this->createMock(Connection::class)));

        $columnConverter = new ColumnConverter();
        // define table info
        $tableInfoColumns = [];
        $tableInfoColumns[] = new TeradataColumn('id', new Teradata('INT', [
            'length' => '',
            'nullable' => false,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('name', new Teradata('VARCHAR', [
            'length' => '100',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('height', new Teradata('DECIMAL', [
            'length' => '4,2',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new TeradataColumn('birth_at', new Teradata('DATE', [
            'length' => '',
            'nullable' => true,
            'default' => '',
        ]));

        // create query builder
        $qb = new ExportQueryBuilder($connection, $columnConverter);

        // build query
        $this->expectException($exceptionClass);
        $this->expectExceptionMessage($exceptionMessage);
        $qb->buildQueryFromCommand(
            $previewCommand->getFilters(),
            $previewCommand->getOrderBy(),
            $previewCommand->getColumns(),
            new ColumnCollection($tableInfoColumns),
            'some_schema',
            '',
            true
        );
    }

    public function provideFailedData(): Generator
    {
        yield 'unsupported dataType' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::BIGINT,
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            QueryBuilderException::class,
            'Data type BIGINT not recognized. Possible datatypes are [INTEGER|REAL]',
        ];
        yield 'fulltext + filter' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => 'word',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::eq,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            QueryBuilderException::class,
            'Cannot use fulltextSearch and whereFilters at the same time',
        ];
        yield 'filter with multiple values and GT operator' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::gt,
                            'values' => ['foo', 'bar'],
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            QueryBuilderException::class,
            'whereFilter with multiple values can be used only with "eq", "ne" operators',
        ];
    }
}
