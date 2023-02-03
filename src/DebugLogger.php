<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Psr\Log\AbstractLogger;
use Stringable;

class DebugLogger extends AbstractLogger
{
    /**
     * @inheritDoc
     * @param string $level
     * @param mixed[] $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        error_log(
            sprintf(
                '%s: %s',
                strtoupper($level),
                $this->interpolate($message, $context)
            )
        );
    }

    /**
     * @param mixed[] $context
     */
    protected function interpolate(string|Stringable $message, array $context = []): string
    {
        $message = (string) $message;
        // build a replacement array with braces around the context keys
        $replace = [];
        foreach ($context as $key => $val) {
            if (is_scalar($val)) {
                $replace['{' . $key . '}'] = $val;
            }
        }

        // interpolate replacement values into the message and return
        return strtr($message, $replace);
    }
}
