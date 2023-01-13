<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;

class NoSpaceException extends Exception
{
    public function __construct(string $msg)
    {
        return parent::__construct($msg, ExceptionInterface::ERR_RESOURCE_FULL);
    }

    public static function createForFullDB(): self
    {
        return new self('DB is full');
    }

    public static function createForFullParent(): self
    {
        return new self('Cannot create database bacause parent database is full');
    }
}
