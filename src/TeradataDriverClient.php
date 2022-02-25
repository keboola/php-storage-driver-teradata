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
use Keboola\StorageDriver\Contract\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
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
    ) {
        assert($credentials instanceof GenericBackendCredentials);
        $handler = $this->getHandler($command);
        return $handler(
            $credentials,
            $command,
            $features
        );
    }

    private function getHandler(Message $command): DriverCommandHandlerInterface
    {
        switch (true) {
            case $command instanceof InitBackendCommand:
                return new InitBackendHandler();
            case $command instanceof RemoveBackendCommand:
                return new RemoveBackendHandler();
            case $command instanceof CreateProjectCommand:
                return new CreateProjectHandler();
            case $command instanceof CreateBucketCommand:
                return new CreateBucketHandler();
            case $command instanceof DropBucketCommand:
                return new DropBucketHandler();
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
