<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/info.proto

namespace Keboola\StorageDriver\Command\Info;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.info.ObjectInfo</code>
 */
class ObjectInfo extends \Google\Protobuf\Internal\Message
{
    /**
     * Object name
     *
     * Generated from protobuf field <code>string objectName = 1;</code>
     */
    protected $objectName = '';
    /**
     * Type of object
     *
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     */
    protected $objectType = 0;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $objectName
     *           Object name
     *     @type int $objectType
     *           Type of object
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Info::initOnce();
        parent::__construct($data);
    }

    /**
     * Object name
     *
     * Generated from protobuf field <code>string objectName = 1;</code>
     * @return string
     */
    public function getObjectName()
    {
        return $this->objectName;
    }

    /**
     * Object name
     *
     * Generated from protobuf field <code>string objectName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setObjectName($var)
    {
        GPBUtil::checkString($var, True);
        $this->objectName = $var;

        return $this;
    }

    /**
     * Type of object
     *
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     * @return int
     */
    public function getObjectType()
    {
        return $this->objectType;
    }

    /**
     * Type of object
     *
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     * @param int $var
     * @return $this
     */
    public function setObjectType($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Info\ObjectType::class);
        $this->objectType = $var;

        return $this;
    }

}

