<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials\Meta;

class RedshiftCredentialsMeta implements CredentialsMetaInterface
{
    private string $database;

    public function __construct(
        string $database
    ) {
        $this->database = $database;
    }

    public function toArray(): array
    {
        return [
            'database' => $this->getDatabase(),
        ];
    }

    public function getDatabase(): string
    {
        return $this->database;
    }
}
