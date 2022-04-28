<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table\TableExportToFileCommand;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta</code>
 */
class TeradataTableExportMeta extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter exportAdapter = 1;</code>
     */
    protected $exportAdapter = 0;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type int $exportAdapter
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter exportAdapter = 1;</code>
     * @return int
     */
    public function getExportAdapter()
    {
        return $this->exportAdapter;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter exportAdapter = 1;</code>
     * @param int $var
     * @return $this
     */
    public function setExportAdapter($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Table\TableExportToFileCommand\TeradataTableExportMeta\ExportAdapter::class);
        $this->exportAdapter = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(TeradataTableExportMeta::class, \Keboola\StorageDriver\Command\Table\TableExportToFileCommand_TeradataTableExportMeta::class);

