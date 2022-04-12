<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class TeradataSessionManager
{
    /**
     * @var array<array{connection: Connection, sessionId: int, user: string}>
     */
    private array $sessions = [];

    private bool $debug;

    public function __construct(bool $debug = false)
    {
        $this->debug = $debug;
    }

    public function createSession(GenericBackendCredentials $credentials): Connection
    {
        $db = ConnectionFactory::getConnection($credentials, $this->debug);
        $sessionId = $db->fetchOne('SELECT SESSION;');
        if (is_numeric($sessionId)) {
            $sessionId = (int) $sessionId;
        } else {
            $sessionId = 0;
        }
        $this->sessions[] = [
            'connection' => $db,
            'sessionId' => $sessionId,
            'user' => $credentials->getPrincipal(),
        ];

        return $db;
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        foreach ($this->sessions as $session) {
            $session['connection']->close();
        }
    }
}
