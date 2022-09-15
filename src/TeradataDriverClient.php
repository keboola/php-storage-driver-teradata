<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Common\TerminateSessionCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Backend\TerminateSessionHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Clear\ClearWorkspaceHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\DropObject\DropWorkspaceObjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Workspace\ResetPassword\ResetWorkspacePasswordHandler;
use Psr\Log\LoggerInterface;

class TeradataDriverClient implements ClientInterface
{
    /** @var callable|null */
    private $sessionCallback;

    private ?LoggerInterface $debugLogger;

    private bool $debug;

    public function __construct(
        bool $debug = false,
        ?callable $sessionCallback = null,
        ?LoggerInterface $debugLogger = null
    ) {
        $this->sessionCallback = $sessionCallback;
        $this->debugLogger = $debugLogger;
        $this->debug = $debug;
    }

    /**
     * @inheritDoc
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        $manager = new TeradataSessionManager(
            $this->debug,
            $this->sessionCallback,
            $this->debugLogger
        );
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
            case $command instanceof DropProjectCommand:
                return new DropProjectHandler($manager);
            case $command instanceof CreateBucketCommand:
                return new CreateBucketHandler($manager);
            case $command instanceof DropBucketCommand:
                return new DropBucketHandler($manager);
            case $command instanceof CreateTableCommand:
                return new CreateTableHandler($manager);
            case $command instanceof DropTableCommand:
                return new DropTableHandler($manager);
            case $command instanceof TableImportFromTableCommand:
                return new ImportTableFromTableHandler($manager);
            case $command instanceof TableImportFromFileCommand:
                return new ImportTableFromFileHandler($manager);
            case $command instanceof PreviewTableCommand:
                return new PreviewTableHandler($manager);
            case $command instanceof TableExportToFileCommand:
                return new ExportTableToFileHandler($manager);
            case $command instanceof CreateWorkspaceCommand:
                return new CreateWorkspaceHandler($manager);
            case $command instanceof DropWorkspaceCommand:
                return new DropWorkspaceHandler($manager);
            case $command instanceof ResetWorkspacePasswordCommand:
                return new ResetWorkspacePasswordHandler($manager);
            case $command instanceof ClearWorkspaceCommand:
                return new ClearWorkspaceHandler($manager);
            case $command instanceof DropWorkspaceObjectCommand:
                return new DropWorkspaceObjectHandler($manager);
            case $command instanceof ObjectInfoCommand:
                return new ObjectInfoHandler($manager);
            case $command instanceof TerminateSessionCommand:
                return new TerminateSessionHandler($manager);
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
