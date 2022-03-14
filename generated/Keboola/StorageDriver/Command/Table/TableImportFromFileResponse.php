<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 **
 * Response of TableImportFromFileCommand
 *
 * Generated from protobuf message <code>keboola.storageDriver.command.table.TableImportFromFileResponse</code>
 */
class TableImportFromFileResponse extends \Google\Protobuf\Internal\Message
{
    /**
     * number of rows imported into final table
     *
     * Generated from protobuf field <code>int64 importedRowsCount = 1;</code>
     */
    protected $importedRowsCount = 0;
    /**
     * list of timers captured by driver
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.table.TableImportFromFileResponse.Timer timers = 2;</code>
     */
    private $timers;
    /**
     * list of imported columns
     *
     * Generated from protobuf field <code>repeated string importedColumns = 3;</code>
     */
    private $importedColumns;
    /**
     * additional import data
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 4;</code>
     */
    protected $meta = null;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type int|string $importedRowsCount
     *           number of rows imported into final table
     *     @type \Keboola\StorageDriver\Command\Table\TableImportFromFileResponse\Timer[]|\Google\Protobuf\Internal\RepeatedField $timers
     *           list of timers captured by driver
     *     @type string[]|\Google\Protobuf\Internal\RepeatedField $importedColumns
     *           list of imported columns
     *     @type \Google\Protobuf\Any $meta
     *           additional import data
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
        parent::__construct($data);
    }

    /**
     * number of rows imported into final table
     *
     * Generated from protobuf field <code>int64 importedRowsCount = 1;</code>
     * @return int|string
     */
    public function getImportedRowsCount()
    {
        return $this->importedRowsCount;
    }

    /**
     * number of rows imported into final table
     *
     * Generated from protobuf field <code>int64 importedRowsCount = 1;</code>
     * @param int|string $var
     * @return $this
     */
    public function setImportedRowsCount($var)
    {
        GPBUtil::checkInt64($var);
        $this->importedRowsCount = $var;

        return $this;
    }

    /**
     * list of timers captured by driver
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.table.TableImportFromFileResponse.Timer timers = 2;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getTimers()
    {
        return $this->timers;
    }

    /**
     * list of timers captured by driver
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.table.TableImportFromFileResponse.Timer timers = 2;</code>
     * @param \Keboola\StorageDriver\Command\Table\TableImportFromFileResponse\Timer[]|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setTimers($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::MESSAGE, \Keboola\StorageDriver\Command\Table\TableImportFromFileResponse\Timer::class);
        $this->timers = $arr;

        return $this;
    }

    /**
     * list of imported columns
     *
     * Generated from protobuf field <code>repeated string importedColumns = 3;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getImportedColumns()
    {
        return $this->importedColumns;
    }

    /**
     * list of imported columns
     *
     * Generated from protobuf field <code>repeated string importedColumns = 3;</code>
     * @param string[]|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setImportedColumns($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::STRING);
        $this->importedColumns = $arr;

        return $this;
    }

    /**
     * additional import data
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 4;</code>
     * @return \Google\Protobuf\Any|null
     */
    public function getMeta()
    {
        return $this->meta;
    }

    public function hasMeta()
    {
        return isset($this->meta);
    }

    public function clearMeta()
    {
        unset($this->meta);
    }

    /**
     * additional import data
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 4;</code>
     * @param \Google\Protobuf\Any $var
     * @return $this
     */
    public function setMeta($var)
    {
        GPBUtil::checkMessage($var, \Google\Protobuf\Any::class);
        $this->meta = $var;

        return $this;
    }

}
