<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

interface CommandMetaInterface
{
    /**
     * @return array<mixed>
     */
    public function toArray(): array;
}
