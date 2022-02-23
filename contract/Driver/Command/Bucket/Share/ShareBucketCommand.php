<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Bucket\Share;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class ShareBucketCommand extends AbstractCommand
{
    public const NAME = 'bucket:share';

    private string $name;

    private string $bucketObjectName;

    private string $bucketShareRoleName;

    private string $projectReadOnlyRole;

    private ?CommandMetaInterface $meta;

    public function __construct(
        string $name,
        string $bucketObjectName,
        string $bucketShareRoleName,
        string $projectReadOnlyRole,
        ?CommandMetaInterface $meta = null
    ) {
        $this->name = $name;
        $this->bucketObjectName = $bucketObjectName;
        $this->bucketShareRoleName = $bucketShareRoleName;
        $this->projectReadOnlyRole = $projectReadOnlyRole;
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        $return = [
            'name' => $this->name,
            'bucketObjectName' => $this->bucketObjectName,
            'bucketShareRoleName' => $this->bucketShareRoleName,
            'projectReadOnlyRole' => $this->projectReadOnlyRole,
        ];

        if ($this->meta !== null) {
            $return['meta'] = $this->meta->toArray();
        }
        return $return;
    }
}
