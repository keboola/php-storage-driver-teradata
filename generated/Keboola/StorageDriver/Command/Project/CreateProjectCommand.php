<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/project.proto

namespace Keboola\StorageDriver\Command\Project;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.project.CreateProjectCommand</code>
 */
class CreateProjectCommand extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     */
    protected $stackPrefix = '';
    /**
     * Generated from protobuf field <code>string projectId = 2;</code>
     */
    protected $projectId = '';
    /**
     * Generated from protobuf field <code>.google.protobuf.Any meta = 5;</code>
     */
    protected $meta = null;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $stackPrefix
     *     @type string $projectId
     *     @type \Google\Protobuf\Any $meta
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Project::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     * @return string
     */
    public function getStackPrefix()
    {
        return $this->stackPrefix;
    }

    /**
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setStackPrefix($var)
    {
        GPBUtil::checkString($var, True);
        $this->stackPrefix = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string projectId = 2;</code>
     * @return string
     */
    public function getProjectId()
    {
        return $this->projectId;
    }

    /**
     * Generated from protobuf field <code>string projectId = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setProjectId($var)
    {
        GPBUtil::checkString($var, True);
        $this->projectId = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.google.protobuf.Any meta = 5;</code>
     * @return \Google\Protobuf\Any
     */
    public function getMeta()
    {
        return $this->meta;
    }

    /**
     * Generated from protobuf field <code>.google.protobuf.Any meta = 5;</code>
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

