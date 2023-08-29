<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\Handler\Exception\ExceptionResolver;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

final class GrantBucketAccessToReadOnlyRoleHandler implements DriverCommandHandlerInterface
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
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof GrantBucketAccessToReadOnlyRoleCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.projectReadOnlyRoleName is required'
        );
        assert(
            $command->getBucketObjectName() !== '',
            'GrantBucketAccessToReadOnlyRoleCommand.bucketObjectName is required'
        );


        $db = $this->manager->createSession($credentials);
        try {
            $db->executeQuery(sprintf(
                'GRANT SELECT ON %s TO %s',
                TeradataQuote::quoteSingleIdentifier($command->getBucketObjectName()),
                TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
            ));
        } catch (Throwable $e) {
            throw ExceptionResolver::resolveException($e);
        }

        return null;
    }
}
