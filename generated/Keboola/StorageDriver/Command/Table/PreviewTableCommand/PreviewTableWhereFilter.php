<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table\PreviewTableCommand;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableWhereFilter</code>
 */
class PreviewTableWhereFilter extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string columnsName = 1;</code>
     */
    protected $columnsName = '';
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableWhereFilter.Operator operator = 2;</code>
     */
    protected $operator = 0;
    /**
     * Generated from protobuf field <code>repeated string values = 3;</code>
     */
    private $values;
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.DataType dataType = 4;</code>
     */
    protected $dataType = 0;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $columnsName
     *     @type int $operator
     *     @type string[]|\Google\Protobuf\Internal\RepeatedField $values
     *     @type int $dataType
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string columnsName = 1;</code>
     * @return string
     */
    public function getColumnsName()
    {
        return $this->columnsName;
    }

    /**
     * Generated from protobuf field <code>string columnsName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setColumnsName($var)
    {
        GPBUtil::checkString($var, True);
        $this->columnsName = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableWhereFilter.Operator operator = 2;</code>
     * @return int
     */
    public function getOperator()
    {
        return $this->operator;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableWhereFilter.Operator operator = 2;</code>
     * @param int $var
     * @return $this
     */
    public function setOperator($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Table\PreviewTableCommand_PreviewTableWhereFilter_Operator::class);
        $this->operator = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>repeated string values = 3;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getValues()
    {
        return $this->values;
    }

    /**
     * Generated from protobuf field <code>repeated string values = 3;</code>
     * @param string[]|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setValues($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::STRING);
        $this->values = $arr;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.DataType dataType = 4;</code>
     * @return int
     */
    public function getDataType()
    {
        return $this->dataType;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.PreviewTableCommand.DataType dataType = 4;</code>
     * @param int $var
     * @return $this
     */
    public function setDataType($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Table\PreviewTableCommand_DataType::class);
        $this->dataType = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(PreviewTableWhereFilter::class, \Keboola\StorageDriver\Command\Table\PreviewTableCommand_PreviewTableWhereFilter::class);

