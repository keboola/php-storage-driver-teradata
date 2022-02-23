<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials\Meta;

class SnowflakeCredentialsMeta implements CredentialsMetaInterface
{
    private string $warehouse;

    private int $workspaceStatementTimeoutSeconds;

    private int $tracingLevel;

    private ?string $database;

    public function __construct(
        ?string $database,
        string $warehouse,
        int $workspaceStatementTimeoutSeconds,
        int $tracingLevel = 0
    ) {
        $this->warehouse = $warehouse;
        $this->workspaceStatementTimeoutSeconds = $workspaceStatementTimeoutSeconds;
        $this->tracingLevel = $tracingLevel;
        $this->database = $database;
    }

    public function toArray(): array
    {
        return [
            'database' => $this->getDatabase(),
            'warehouse' => $this->getWarehouse(),
            'tracingLevel' => $this->getTracingLevel(),
            'workspaceStatementTimeoutSeconds' => $this->getWorkspaceStatementTimeoutSeconds(),
        ];
    }

    public function getDatabase(): ?string
    {
        return $this->database;
    }

    public function getWarehouse(): string
    {
        return $this->warehouse;
    }

    public function getTracingLevel(): int
    {
        return $this->tracingLevel;
    }

    public function getWorkspaceStatementTimeoutSeconds(): int
    {
        return $this->workspaceStatementTimeoutSeconds;
    }
}
