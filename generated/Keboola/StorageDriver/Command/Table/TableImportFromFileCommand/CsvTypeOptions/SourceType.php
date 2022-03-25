<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table\TableImportFromFileCommand\CsvTypeOptions;

use UnexpectedValueException;

/**
 **
 * File path can point into different kinds of sources
 *
 * Protobuf type <code>keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions.SourceType</code>
 */
class SourceType
{
    /**
     * single file, path points on single file only
     *
     * Generated from protobuf enum <code>SINGLE_FILE = 0;</code>
     */
    const SINGLE_FILE = 0;
    /**
     * sliced file, path points on manifest file
     *
     * Generated from protobuf enum <code>SLICED_FILE = 1;</code>
     */
    const SLICED_FILE = 1;
    /**
     * directory, path points on directory of files
     *
     * Generated from protobuf enum <code>DIRECTORY = 2;</code>
     */
    const DIRECTORY = 2;

    private static $valueToName = [
        self::SINGLE_FILE => 'SINGLE_FILE',
        self::SLICED_FILE => 'SLICED_FILE',
        self::DIRECTORY => 'DIRECTORY',
    ];

    public static function name($value)
    {
        if (!isset(self::$valueToName[$value])) {
            throw new UnexpectedValueException(sprintf(
                    'Enum %s has no name defined for value %s', __CLASS__, $value));
        }
        return self::$valueToName[$value];
    }


    public static function value($name)
    {
        $const = __CLASS__ . '::' . strtoupper($name);
        if (!defined($const)) {
            throw new UnexpectedValueException(sprintf(
                    'Enum %s has no value defined for name %s', __CLASS__, $name));
        }
        return constant($const);
    }
}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(SourceType::class, \Keboola\StorageDriver\Command\Table\TableImportFromFileCommand_CsvTypeOptions_SourceType::class);

