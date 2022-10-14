<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Shared\Utils\TemporalSignalLogger;

class DriverTask
{
    private DriverRequest $req;

    private string $workflowId;

    public function __construct(
        DriverRequest $req,
        string $workflowId
    ) {
        $this->workflowId = $workflowId;
        $this->req = $req;
    }

    public function run(): ?\Google\Protobuf\Internal\Message
    {
        $workflowClient = \Temporal\Client\WorkflowClient::create(
            \Temporal\Client\GRPC\ServiceClient::create('localhost:7233')
        );
        $workflow = $workflowClient->newUntypedRunningWorkflowStub(
            $this->workflowId
        );

        return (new TeradataDriverClient(
            true,
            static function (string $sessionId) use ($workflow) {
                $workflow->signal('setSessionId', $sessionId);
            },
            new TemporalSignalLogger($workflow)
        ))->runCommand(
            $this->req->getCredentials()->unpack(),
            $this->req->getCommand()->unpack(),
            iterator_to_array($this->req->getFeatures()),
        );
    }
}
