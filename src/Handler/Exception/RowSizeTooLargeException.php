<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;

class RowSizeTooLargeException extends Exception
{
    public function __construct(string $msg, \Throwable $e)
    {
        parent::__construct($msg, ExceptionInterface::ERR_ROW_SIZE_TOO_LARGE, $e);
    }

    public static function createForTablePermRowHasExceededLimit(\Throwable $e): self
    {
        return new self('Row size is too large.', $e);
    }
}
