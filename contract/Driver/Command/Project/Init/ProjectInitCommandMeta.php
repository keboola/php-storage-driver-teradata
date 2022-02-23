<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Project\Init;

use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class ProjectInitCommandMeta implements CommandMetaInterface
{
    private ?string $readOnlyRoleName;

    public function __construct(
        ?string $readOnlyRoleName
    ) {
        $this->readOnlyRoleName = $readOnlyRoleName;
    }

    public function toArray(): array
    {
        return [
            'readOnlyRoleName' => $this->readOnlyRoleName,
        ];
    }
}
