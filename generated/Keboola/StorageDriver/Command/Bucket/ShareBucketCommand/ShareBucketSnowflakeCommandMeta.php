<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/bucket.proto

namespace Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.bucket.ShareBucketCommand.ShareBucketSnowflakeCommandMeta</code>
 */
class ShareBucketSnowflakeCommandMeta extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string databaseName = 1;</code>
     */
    protected $databaseName = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $databaseName
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Bucket::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string databaseName = 1;</code>
     * @return string
     */
    public function getDatabaseName()
    {
        return $this->databaseName;
    }

    /**
     * Generated from protobuf field <code>string databaseName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setDatabaseName($var)
    {
        GPBUtil::checkString($var, True);
        $this->databaseName = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(ShareBucketSnowflakeCommandMeta::class, \Keboola\StorageDriver\Command\Bucket\ShareBucketCommand_ShareBucketSnowflakeCommandMeta::class);

