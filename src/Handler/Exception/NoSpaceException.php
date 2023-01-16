<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;

class NoSpaceException extends Exception
{
    public function __construct(string $msg, \Throwable $e)
    {
        parent::__construct($msg, ExceptionInterface::ERR_RESOURCE_FULL, $e);
    }

    public static function createForFullDB(\Throwable $e): self
    {
        return new self('Database is full. Cannot insert data or create new objects.', $e);
    }

    public static function createForFullParent(\Throwable $e): self
    {
        return new self('Cannot create database because parent database is full.', $e);
    }
}
