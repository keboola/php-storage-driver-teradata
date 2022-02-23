<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Backend\Init;

use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class InitBackendSynapseCommandMeta implements CommandMetaInterface
{
    private string $globalRoleName;

    public function __construct(
        string $globalRoleName
    ) {
        $this->globalRoleName = $globalRoleName;
    }

    public function toArray(): array
    {
        return [
            'globalRoleName' => $this->globalRoleName,
        ];
    }
}
