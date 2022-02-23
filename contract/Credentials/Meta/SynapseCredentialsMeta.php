<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials\Meta;

class SynapseCredentialsMeta implements CredentialsMetaInterface
{
    private string $database;

    private bool $useManagedIdentity;

    public function __construct(
        string $database,
        bool $useManagedIdentity
    ) {
        $this->database = $database;
        $this->useManagedIdentity = $useManagedIdentity;
    }

    public function useManagedIdentity(): bool
    {
        return $this->useManagedIdentity;
    }

    public function toArray(): array
    {
        return [
            'database' => $this->getDatabase(),
            'useManagedIdentity' => $this->useManagedIdentity(),
        ];
    }

    public function getDatabase(): string
    {
        return $this->database;
    }
}
