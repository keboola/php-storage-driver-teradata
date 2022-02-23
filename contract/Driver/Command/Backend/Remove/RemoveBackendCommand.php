<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Backend\Remove;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class RemoveBackendCommand extends AbstractCommand
{
    public const NAME = 'backend:remove';

    private ?CommandMetaInterface $meta;

    public function __construct(?CommandMetaInterface $meta)
    {
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        $data = [];
        if ($this->meta !== null) {
            $data['meta'] = $this->meta->toArray();
        }
        return $data;
    }
}
