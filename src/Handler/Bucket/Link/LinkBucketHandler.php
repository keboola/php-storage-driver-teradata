<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Link;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class LinkBucketHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof LinkBucketCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        $db = $this->manager->createSession($credentials);

        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getSourceShareRoleName()),
            TeradataQuote::quoteSingleIdentifier($command->getTargetProjectReadOnlyRoleName()),
        ));

        // returning empty response to be compatible with BQ
        return new LinkedBucketResponse();
    }
}
