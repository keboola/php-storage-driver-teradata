<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\QueryBuilder;

use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

class ColumnConverter
{
    public const DATA_TYPES_OPTIONS = [
        DataType::INTEGER,
        DataType::REAL,
    ];

    public const DATA_TYPES_MAP = [
        DataType::STRING => Teradata::TYPE_VARCHAR,
        DataType::INTEGER => Teradata::TYPE_INTEGER,
        DataType::DOUBLE => Teradata::TYPE_DOUBLE_PRECISION,
        DataType::BIGINT => Teradata::TYPE_BIGINT,
        DataType::REAL => Teradata::TYPE_REAL,
        DataType::DECIMAL => Teradata::TYPE_DECIMAL,
    ];

    /**
     * Only cast STRING type to a given NUMERIC type
     */
    public function convertColumnByDataType(string $column, int $dataType): string
    {
        if (!in_array($dataType, self::DATA_TYPES_OPTIONS, true)) {
            throw new TableFilterQueryBuilderException(
                sprintf(
                    'Data type %s not recognized. Possible datatypes are [%s]',
                    self::DATA_TYPES_MAP[$dataType],
                    implode('|', array_map(
                        static fn (int $type) => self::DATA_TYPES_MAP[$type],
                        self::DATA_TYPES_OPTIONS,
                    ))
                ),
            );
        }
        if ($dataType === DataType::INTEGER) {
            return sprintf(
                'CAST(TO_NUMBER(%s) AS INTEGER)',
                TeradataQuote::quoteSingleIdentifier($column),
            );
        }
        return sprintf(
            'CAST(TO_NUMBER(%s) AS REAL)',
            TeradataQuote::quoteSingleIdentifier($column),
        );
    }
}
