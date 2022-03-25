<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/workspace.proto

namespace Keboola\StorageDriver\Command\Workspace;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 **
 * Command will remove workspace content
 *
 * Generated from protobuf message <code>keboola.storageDriver.command.workspace.ResetWorkspacePasswordCommand</code>
 */
class ResetWorkspacePasswordCommand extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string workspaceUserName = 1;</code>
     */
    protected $workspaceUserName = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $workspaceUserName
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Workspace::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string workspaceUserName = 1;</code>
     * @return string
     */
    public function getWorkspaceUserName()
    {
        return $this->workspaceUserName;
    }

    /**
     * Generated from protobuf field <code>string workspaceUserName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setWorkspaceUserName($var)
    {
        GPBUtil::checkString($var, True);
        $this->workspaceUserName = $var;

        return $this;
    }

}

