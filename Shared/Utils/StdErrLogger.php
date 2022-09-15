<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

use Psr\Log\LoggerInterface;
use Psr\Log\LoggerTrait;

class StdErrLogger implements LoggerInterface
{
    use LoggerTrait;

    /**
     * @param mixed   $level
     * @param string  $message
     * @param mixed[] $context
     */
    public function log($level, $message, array $context = array()): void
    {
        file_put_contents('php://stderr', sprintf('[%s] %s', $level, $message));
    }
}
