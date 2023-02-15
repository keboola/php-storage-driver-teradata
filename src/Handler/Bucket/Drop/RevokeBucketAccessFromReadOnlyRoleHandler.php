<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\Handler\Exception\ExceptionResolver;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

final class RevokeBucketAccessFromReadOnlyRoleHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof RevokeBucketAccessFromReadOnlyRoleCommand);
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.projectReadOnlyRoleName is required'
        );
        assert(
            $command->getBucketObjectName() !== '',
            'RevokeBucketAccessToReadOnlyRoleCommand.bucketObjectName is required'
        );

        $db = $this->manager->createSession($credentials);
        $ignoreErrors = $command->getIgnoreErrors();

        try {
            $db->executeStatement(sprintf(
                'REVOKE SELECT ON %s FROM %s',
                TeradataQuote::quoteSingleIdentifier($command->getBucketObjectName()),
                TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
            ));
        } catch (Throwable $e) {
            if (!$ignoreErrors) {
                throw ExceptionResolver::resolveException($e);
            }
        }

        return null;
    }
}
