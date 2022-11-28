<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableFilterQueryBuilder;
use Keboola\StorageDriver\Teradata\QueryBuilder\TableFilterQueryBuilderException;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataPlatform;
use PHPUnit\Framework\TestCase;

class TableFilterQueryBuilderTest extends TestCase
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
        $tableInfoColumns = new RepeatedField(GPBType::MESSAGE, TableInfo\TableColumn::class);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'id',
            'type' => 'INT',
            'length' => '',
            'nullable' => false,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'name',
            'type' => 'VARCHAR',
            'length' => '100',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'height',
            'type' => 'DECIMAL',
            'length' => '4,2',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'birth_at',
            'type' => 'DATE',
            'length' => '',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfo = (new TableInfo())
            ->setPath(ProtobufHelper::arrayToRepeatedString(['some_schema']))
            ->setTableName('some_table')
            ->setColumns($tableInfoColumns)
            ->setPrimaryKeysNames(ProtobufHelper::arrayToRepeatedString(['id']));

        // create query builder
        $qb = new TableFilterQueryBuilder($connection, $tableInfo, $columnConverter);

        // build query
        $queryData = $qb->buildQueryFromCommand($previewCommand, 'some_schema');

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
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 100,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'name',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['foo'],
                        'dataType' => DataType::STRING,
                    ]),
                ],
                'orderBy' => new PreviewTableCommand\PreviewTableOrderBy([
                    'columnName' => 'name',
                    'order' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC,
                    'dataType' => DataType::STRING,
                ]),
            ]),
            <<<SQL
            SELECT TOP 100 "id", "name" FROM "some_schema"."some_table"
             WHERE ("name" <> :dcValue1) OR ("name" IS NULL)
             ORDER BY "name" ASC
            SQL,
            [
                'dcValue1' => 'foo',
            ],
            [
                'dcValue1' => ParameterType::STRING,
            ],
        ];
        yield 'more filters + orderBy' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'name',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['foo'],
                        'dataType' => DataType::STRING,
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'height',
                        'operator' => TableWhereFilter\Operator::ge,
                        'values' => ['1.23'],
                        'dataType' => DataType::STRING,
                    ]),
                ],
                'orderBy' => new PreviewTableCommand\PreviewTableOrderBy([
                    'columnName' => 'id',
                    'order' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC,
                    'dataType' => DataType::STRING,
                ]),
            ]),
            <<<SQL
            SELECT "id", "name" FROM "some_schema"."some_table"
             WHERE (("name" <> :dcValue1) OR ("name" IS NULL))
             AND ("height" >= :dcValue2)
             ORDER BY "id" ASC
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
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'fulltextSearch' => 'foo',
                'whereFilters' => [],
                'orderBy' => null,
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
                'limit' => 0,
                'changeSince' => '1667293200',
                'changeUntil' => '1669827600',
                'columns' => ['id', 'name'],
                'fulltextSearch' => '',
                'whereFilters' => [],
                'orderBy' => null,
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
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'height',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['10.20'],
                        'dataType' => DataType::REAL,
                    ]),
                ],
                'orderBy' => new PreviewTableCommand\PreviewTableOrderBy([
                    'columnName' => 'id',
                    'order' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC,
                    'dataType' => DataType::REAL,
                ]),
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE (CAST(TO_NUMBER("height") AS REAL) <> :dcValue1) OR ("height" IS NULL)
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
                'tableName' => 'some_table',
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'id',
                        'operator' => TableWhereFilter\Operator::eq,
                        'values' => ['foo', 'bar'],
                        'dataType' => DataType::STRING,
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'id',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['50', '60'],
                        'dataType' => DataType::INTEGER,
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'height',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['10.20'],
                        'dataType' => DataType::REAL,
                    ]),
                ],
                'orderBy' => null,
            ]),
            <<<SQL
            SELECT "id", "name", "height", "birth_at" FROM "some_schema"."some_table"
             WHERE ("id" IN ('foo','bar'))
             AND ((CAST(TO_NUMBER("id") AS INTEGER) NOT IN ('50','60')) OR ("id" IS NULL))
             AND ((CAST(TO_NUMBER("height") AS REAL) <> :dcValue1) OR ("height" IS NULL))
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
     * @param class-string<\Exception> $exceptionClass
     */
    public function testBuildQueryFromCommnandFailed(
        PreviewTableCommand $previewCommand,
        string $exceptionClass,
        string $exceptionMessage
    ): void {
        $connection = $this->createMock(Connection::class);
        $connection->method('getExpressionBuilder')
            ->willReturn(new ExpressionBuilder($this->createMock(Connection::class)));
        $connection->method('getDatabasePlatform')
            ->willReturn(new TeradataPlatform());

        $columnConverter = new ColumnConverter();

        // define table info
        $tableInfoColumns = new RepeatedField(GPBType::MESSAGE, TableInfo\TableColumn::class);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'id',
            'type' => 'INT',
            'length' => '',
            'nullable' => false,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'name',
            'type' => 'VARCHAR',
            'length' => '100',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'height',
            'type' => 'DECIMAL',
            'length' => '4,2',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfoColumns[] = new TableInfo\TableColumn([
            'name' => 'birth_at',
            'type' => 'DATE',
            'length' => '',
            'nullable' => true,
            'default' => '',
        ]);
        $tableInfo = (new TableInfo())
            ->setPath(ProtobufHelper::arrayToRepeatedString(['some_schema']))
            ->setTableName('some_table')
            ->setColumns($tableInfoColumns)
            ->setPrimaryKeysNames(ProtobufHelper::arrayToRepeatedString(['id']));

        // create query builder
        $qb = new TableFilterQueryBuilder($connection, $tableInfo, $columnConverter);

        // build query
        $this->expectException($exceptionClass);
        $this->expectExceptionMessage($exceptionMessage);
        $qb->buildQueryFromCommand($previewCommand, 'some_schema');
    }

    public function provideFailedData(): Generator
    {
        yield 'unsupported dataType' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'name',
                        'operator' => TableWhereFilter\Operator::ne,
                        'values' => ['foo'],
                        'dataType' => DataType::BIGINT,
                    ]),
                ],
                'orderBy' => null,
            ]),
            TableFilterQueryBuilderException::class,
            'Data type BIGINT not recognized. Possible datatypes are [INTEGER|REAL]',
        ];
        yield 'fulltext + filter' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => 'word',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'name',
                        'operator' => TableWhereFilter\Operator::eq,
                        'values' => ['foo'],
                        'dataType' => DataType::STRING,
                    ]),
                ],
                'orderBy' => null,
            ]),
            TableFilterQueryBuilderException::class,
            'Cannot use fulltextSearch and whereFilters at the same time',
        ];
        yield 'filter with empty string and GT operator' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 0,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => '',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'name',
                        'operator' => TableWhereFilter\Operator::gt,
                        'values' => [''],
                        'dataType' => DataType::STRING,
                    ]),
                ],
                'orderBy' => null,
            ]),
            TableFilterQueryBuilderException::class,
            'Teradata where filter on empty strings can be used only with "ne, eq" operators.',
        ];
    }
}
