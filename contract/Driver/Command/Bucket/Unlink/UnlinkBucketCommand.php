<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Bucket\Unlink;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class UnlinkBucketCommand extends AbstractCommand
{
    public const NAME = 'bucket:link';

    private string $name;

    private string $sourceShareRole;

    private string $projectReadOnlyRole;

    private ?CommandMetaInterface $meta;

    public function __construct(
        string $name,
        string $sourceShareRole,
        string $projectReadOnlyRole,
        ?CommandMetaInterface $meta = null
    ) {
        $this->name = $name;
        $this->sourceShareRole = $sourceShareRole;
        $this->projectReadOnlyRole = $projectReadOnlyRole;
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        $return = [
            'name' => $this->name,
            'sourceShareRole' => $this->sourceShareRole,
            'projectReadOnlyRole' => $this->projectReadOnlyRole,
        ];
        if ($this->meta !== null) {
            $return['meta'] = $this->meta->toArray();
        }
        return $return;
    }
}
