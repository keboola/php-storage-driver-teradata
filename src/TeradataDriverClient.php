<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;

class TeradataDriverClient implements ClientInterface
{
    /**
     * @inheritDoc
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        $manager = new TeradataSessionManager();
        $handler = $this->getHandler($command, $manager);
        try {
            $response = $handler(
                $credentials,
                $command,
                $features
            );
        } finally {
            $manager->close();
        }

        return $response;
    }

    private function getHandler(Message $command, TeradataSessionManager $manager): DriverCommandHandlerInterface
    {
        switch (true) {
            case $command instanceof InitBackendCommand:
                return new InitBackendHandler($manager);
            case $command instanceof RemoveBackendCommand:
                return new RemoveBackendHandler();
            case $command instanceof CreateProjectCommand:
                return new CreateProjectHandler($manager);
            case $command instanceof CreateBucketCommand:
                return new CreateBucketHandler($manager);
            case $command instanceof DropBucketCommand:
                return new DropBucketHandler($manager);
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
