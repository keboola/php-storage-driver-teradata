<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

abstract class AbstractCommand implements DriverCommandInterface
{
    public const NAME = '!!!!NAME OF COMMAND IS MISSING!!!!';

    public static function getCommandName(): string
    {
        return self::NAME;
    }
}
