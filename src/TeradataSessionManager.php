<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Psr\Log\LoggerInterface;

class TeradataSessionManager
{
    /**
     * @var array<array{connection: Connection, sessionId: int, user: string}>
     */
    private array $sessions = [];

    private bool $debug;

    /** @var callable|null */
    private $sessionCallback;

    private ?LoggerInterface $debugLogger;

    public function __construct(
        bool $debug = false,
        ?callable $sessionCallback = null,
        ?LoggerInterface $debugLogger = null
    ) {
        $this->debug = $debug;
        $this->sessionCallback = $sessionCallback;
        $this->debugLogger = $debugLogger;
    }

    public function createSession(GenericBackendCredentials $credentials): Connection
    {
        $db = ConnectionFactory::getConnection(
            $credentials,
            $this->debug,
            $this->debugLogger
        );
        $sessionId = $db->fetchOne('SELECT SESSION;');
        if (is_numeric($sessionId)) {
            $sessionId = (int) $sessionId;
            if ($this->sessionCallback !== null) {
                call_user_func($this->sessionCallback, (string) $sessionId);
            }
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
