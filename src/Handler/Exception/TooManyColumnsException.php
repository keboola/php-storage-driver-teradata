<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;

class TooManyColumnsException extends Exception
{
    public function __construct(string $msg, \Throwable $e)
    {
        parent::__construct($msg, ExceptionInterface::ERR_ROW_SIZE_TOO_LARGE, $e);
    }

    public static function createForTooManyColumns(\Throwable $e): self
    {
        return new self('Too many columns. Maximum is 2047 columns.', $e);
    }
}
