<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver;

use Keboola\StorageDriver\Contract\Credentials\CredentialsInterface;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Init\InitBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Remove\RemoveBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Bucket\Create\CreateBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandInterface;

interface ClientInterface
{
    // backend
    public const BACKEND__INIT = InitBackendCommand::NAME;
    public const BACKEND__REMOVE = RemoveBackendCommand::NAME;
    // project
    public const PROJECT__INIT = 'project:init';
    public const PROJECT__REMOVE = 'project:remove';
    // reflection
    public const OBJECT__INFO = 'object:info';
    // bucket
    public const BUCKET__CREATE = CreateBucketCommand::NAME;
    public const BUCKET__DROP = 'bucket:drop';
    public const BUCKET__INIT_SHARING = 'bucket:init-sharing';
    public const BUCKET__DISABLE_SHARING = 'bucket:disable-sharing';
    public const BUCKET__LINK = 'bucket:link';
    public const BUCKET__UNLINK = 'bucket:unlink';
    // table
    public const TABLE__CREATE = 'table:create';
    public const TABLE__DROP = 'table:drop';
    public const TABLE__ALTER = 'table:alter';
    public const TABLE__PREVIEW = 'table:preview';
    public const TABLE__EXPORT = 'table:export';
    public const TABLE__IMPORT_FROM_FILE = 'table:import-from-file';
    public const TABLE__IMPORT_FROM_TABLE = 'table:import-from-table';
    // workspace
    public const WORKSPACE__CREATE = 'workspace:create';
    public const WORKSPACE__DROP = 'workspace:drop';
    public const WORKSPACE__CLEAR = 'workspace:clear';
    public const WORKSPACE__RESET_PASSWORD = 'workspace:reset-password';
    public const WORKSPACE__DROP_OBJECT = 'workspace:drop-object';
    public const WORKSPACE__GRANT_ACCESS_FOR_ROLE = 'workspace:grant-access-for-role';
    public const WORKSPACE__REVOKE_ACCESS_FOR_ROLE = 'workspace:revoke-access-for-role';

    /**
     * @param string[] $features
     * @return mixed
     */
    public function runCommand(
        string $backend,
        CredentialsInterface $credentials,
        DriverCommandInterface $command,
        array $features
    );
}
