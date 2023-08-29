<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\UnLink;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class UnLinkBucketHandler implements DriverCommandHandlerInterface
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
        array   $features,
        Message $runtimeOptions,
    ): ?Message
    {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof UnlinkBucketCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        $db = $this->manager->createSession($credentials);

        $db->executeStatement(sprintf(
            'REVOKE %s FROM %s;',
            TeradataQuote::quoteSingleIdentifier($command->getSourceShareRoleName()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
        ));

        return null;
    }
}
