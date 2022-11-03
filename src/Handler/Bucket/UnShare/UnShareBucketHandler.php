<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\UnShare;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class UnShareBucketHandler implements DriverCommandHandlerInterface
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
        Message $command, // linked bucket
        array   $features
    ): ?Message
    {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof UnshareBucketCommand);

        $db = $this->manager->createSession($credentials);

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getBucketShareRoleName())
        ));

        return null;
    }
}
