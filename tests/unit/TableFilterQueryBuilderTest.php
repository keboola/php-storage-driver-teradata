<?php

namespace Keboola\StorageDriver\UnitTests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\Expression\ExpressionBuilder;
use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Db\ImportExport\Storage\Teradata\SelectSource;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\TableFilterQueryBuilder;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumnConverter;
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
        PreviewTableCommand $inputCommand,
        string $expectedSql,
        array $expectedBindings,
        array $expectedDataTypes
    ): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('getExpressionBuilder')
            ->willReturn(new ExpressionBuilder($this->createMock(Connection::class)));
        $connection->method('getDatabasePlatform')
            ->willReturn(new TeradataPlatform());

        $columnConverter = new TeradataColumnConverter();

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
            ->setPrimaryKeysNames(ProtobufHelper::arrayToRepeatedString(['id']))
        ;

        // create query builder
        $qb = new TableFilterQueryBuilder($connection, $tableInfo, $columnConverter);

        // define preview command
        $previewCommand = $inputCommand;

        // build query
        /** @var SelectSource $source */
        $source = $qb->buildQueryFromCommnand($previewCommand, 'some_schema');

        dump($source->getQuery());
        dump($source->getQueryBindings());
        dump($source->getDataTypes());

        $this->assertSame(
            str_replace(PHP_EOL, '', $expectedSql),
            $source->getQuery(),
        );
        $this->assertSame(
            $expectedBindings,
            $source->getQueryBindings(),
        );
        $this->assertSame(
            $expectedDataTypes,
            $source->getDataTypes(),
        );
    }

    public function provideSuccessData(): Generator
    {
        yield 'basic' => [
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
             WHERE ("name" != :dcValue1) OR ("name" IS NULL)
             ORDER BY "name" ASC
            SQL,
            [
                'dcValue1' => 'foo',
            ],
            [
                'dcValue1' => ParameterType::STRING,
            ],
        ];
        yield 'full' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'limit' => 100,
                'changeSince' => '',
                'changeUntil' => '',
                'columns' => ['id', 'name'],
                'fulltextSearch' => 'aaa',
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
             WHERE ("name" != :dcValue1) OR ("name" IS NULL)
             ORDER BY "name" ASC
            SQL,
            [
                'dcValue1' => 'foo',
            ],
            [
                'dcValue1' => 2,
            ],
        ];
    }
}
