<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table\PreviewTableResponse\Row;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.table.PreviewTableResponse.Row.Column</code>
 */
class Column extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     */
    protected $columnName = '';
    /**
     * Generated from protobuf field <code>.google.protobuf.Value value = 2;</code>
     */
    protected $value = null;
    /**
     * Generated from protobuf field <code>bool isTruncated = 3;</code>
     */
    protected $isTruncated = false;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $columnName
     *     @type \Google\Protobuf\Value $value
     *     @type bool $isTruncated
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     * @return string
     */
    public function getColumnName()
    {
        return $this->columnName;
    }

    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setColumnName($var)
    {
        GPBUtil::checkString($var, True);
        $this->columnName = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.google.protobuf.Value value = 2;</code>
     * @return \Google\Protobuf\Value|null
     */
    public function getValue()
    {
        return $this->value;
    }

    public function hasValue()
    {
        return isset($this->value);
    }

    public function clearValue()
    {
        unset($this->value);
    }

    /**
     * Generated from protobuf field <code>.google.protobuf.Value value = 2;</code>
     * @param \Google\Protobuf\Value $var
     * @return $this
     */
    public function setValue($var)
    {
        GPBUtil::checkMessage($var, \Google\Protobuf\Value::class);
        $this->value = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>bool isTruncated = 3;</code>
     * @return bool
     */
    public function getIsTruncated()
    {
        return $this->isTruncated;
    }

    /**
     * Generated from protobuf field <code>bool isTruncated = 3;</code>
     * @param bool $var
     * @return $this
     */
    public function setIsTruncated($var)
    {
        GPBUtil::checkBool($var);
        $this->isTruncated = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(Column::class, \Keboola\StorageDriver\Command\Table\PreviewTableResponse_Row_Column::class);

