<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Doctrine\DBAL\Exception\DriverException;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Workspace\ResetPassword\ResetWorkspacePasswordHandler;
use Throwable;

class ResetWorkspacePasswordTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();
        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();
    }

    public function testResetWorkspacePassword(): void
    {
        // create workspace
        [$credentials, $createResponse] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        assert($credentials instanceof GenericBackendCredentials);
        assert($createResponse instanceof CreateWorkspaceResponse);

        // reset password
        $handler = new ResetWorkspacePasswordHandler($this->sessionManager);
        $command = (new ResetWorkspacePasswordCommand())
            ->setWorkspaceUserName($createResponse->getWorkspaceUserName());

        $passwordResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
        assert($passwordResponse instanceof ResetWorkspacePasswordResponse);

        // check original password
        try {
            $this->getConnection($credentials);
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DriverException::class, $e);
            $this->assertStringContainsString(
                '[Teradata][ODBC Teradata Driver][Teradata Database] (210) The UserId, Password or Account is invalid',
                $e->getMessage()
            );
        }

        // check new password
        $credentials->setSecret($passwordResponse->getWorkspacePassword());

        $wsDb = $this->getConnection($credentials);

        $user = $wsDb->fetchOne('SELECT USER;');
        $this->assertSame($createResponse->getWorkspaceUserName(), $user);

        $wsDb->close();
    }
}
