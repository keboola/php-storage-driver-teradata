<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Backend\Init;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class InitBackendCommand extends AbstractCommand
{
    public const NAME = 'backend:init';

    private ?CommandMetaInterface $meta;

    public function __construct(?CommandMetaInterface $meta)
    {
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        $return = [];
        if ($this->meta !== null) {
            $return['meta'] = $this->meta->toArray();
        }
        return $return;
    }
}
