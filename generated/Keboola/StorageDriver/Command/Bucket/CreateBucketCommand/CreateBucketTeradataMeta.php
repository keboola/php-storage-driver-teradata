<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/bucket.proto

namespace Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.bucket.CreateBucketCommand.CreateBucketTeradataMeta</code>
 */
class CreateBucketTeradataMeta extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string permSpace = 1;</code>
     */
    protected $permSpace = '';
    /**
     * Generated from protobuf field <code>string spoolSpace = 2;</code>
     */
    protected $spoolSpace = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $permSpace
     *     @type string $spoolSpace
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Bucket::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string permSpace = 1;</code>
     * @return string
     */
    public function getPermSpace()
    {
        return $this->permSpace;
    }

    /**
     * Generated from protobuf field <code>string permSpace = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setPermSpace($var)
    {
        GPBUtil::checkString($var, True);
        $this->permSpace = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string spoolSpace = 2;</code>
     * @return string
     */
    public function getSpoolSpace()
    {
        return $this->spoolSpace;
    }

    /**
     * Generated from protobuf field <code>string spoolSpace = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setSpoolSpace($var)
    {
        GPBUtil::checkString($var, True);
        $this->spoolSpace = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(CreateBucketTeradataMeta::class, \Keboola\StorageDriver\Command\Bucket\CreateBucketCommand_CreateBucketTeradataMeta::class);

