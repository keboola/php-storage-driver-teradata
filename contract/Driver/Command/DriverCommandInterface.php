<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

interface DriverCommandInterface
{
    public static function getCommandName(): string;

    /**
     * @return array<mixed>
     */
    public function toArray(): array;
}
