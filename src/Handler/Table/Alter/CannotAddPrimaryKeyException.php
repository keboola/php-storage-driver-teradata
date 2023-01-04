<?php

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

class CannotAddPrimaryKeyException extends Exception implements
    ApplicationExceptionInterface,
    NonRetryableExceptionInterface
{
    public function __construct(string $command)
    {
        parent::__construct(sprintf(
            'Command "%s" not supported.',
            $command
        ));
    }

    public static function createForNullableColumn(string $columnName): self
    {
        return new self(sprintf('Selected column %s is nullable', $columnName));
    }

    public static function createForDuplicates(): self
    {
        return new self('Selected columns contain duplicities');
    }

    public static function createForExistingPK(): self
    {
        return new self('Primary key already exists');
    }
}
