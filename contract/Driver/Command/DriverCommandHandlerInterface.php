<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

use Keboola\StorageDriver\Contract\Credentials\CredentialsInterface;

interface DriverCommandHandlerInterface
{
    /**
     * @param string[] $features
     * @return mixed
     */
    public function __invoke(
        CredentialsInterface $credentials,
        DriverCommandInterface $command,
        array $features
    );
}
