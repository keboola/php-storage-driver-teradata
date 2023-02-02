<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\DbUtils;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Throwable;

final class DropBucketHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropBucketCommand);

        $db = $this->manager->createSession($credentials);
        $ignoreErrors = $command->getIgnoreErrors();

        if ($command->getIsCascade() === true) {
            try {
                DbUtils::cleanUserOrDatabase(
                    $db,
                    $command->getBucketObjectName(),
                    $credentials->getPrincipal(),
                );
            } catch (Throwable $e) {
                if (!$ignoreErrors) {
                    $db->close();
                    throw $e;
                }
            }
        }

        $db->close();
        return null;
    }
}
