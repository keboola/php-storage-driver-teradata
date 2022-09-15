<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception;

use Keboola\CommonExceptions\UserExceptionInterface;
use Temporal\Exception\Failure\ApplicationFailure;

/**
 * Exception which output will be displayed to user
 */
class UserException extends ApplicationFailure implements UserExceptionInterface
{
}
