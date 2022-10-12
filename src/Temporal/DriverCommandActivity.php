<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Temporal;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Driver\DriverCommandActivityInterface;
use Keboola\StorageDriver\Shared\Utils\StdErrLogger;
use Keboola\StorageDriver\Shared\Utils\TemporalSignalLogger;
use Keboola\StorageDriver\Teradata\TeradataDriverClient;
use React\EventLoop\Loop;
use Temporal\Activity;
use function React\Async\await;
use function React\Async\coroutine;

class DriverCommandActivity implements DriverCommandActivityInterface
{
    private StdErrLogger $logger;

    public function __construct()
    {
        $this->logger = new StdErrLogger();
    }

    public function executeCommand(DriverRequest $req): DriverResponse
    {
        $info = Activity::getInfo();
        assert($info->workflowExecution !== null);
        $this->log("workflowId=" . $info->workflowExecution->getID());
        $this->log("runId=" . $info->workflowExecution->getRunID());
        $this->log("activityId=" . $info->id);
        $this->log("activityDeadline=" . $info->deadline->format(\DateTimeInterface::ATOM));

        $workflowClient = \Temporal\Client\WorkflowClient::create(
            \Temporal\Client\GRPC\ServiceClient::create('localhost:7233')
        );
        $workflow = $workflowClient->newUntypedRunningWorkflowStub(
            $info->workflowExecution->getID()
        );

        var_export('start heartbeat');

        Loop::addPeriodicTimer(0.01, function (): void {
            Activity::heartbeat('running');
        });

        var_export('run cmd ' . $info->workflowExecution->getID());
        $promise = coroutine(function () use ($req, $workflow) {
            yield (new TeradataDriverClient(
                true,
                static function (string $sessionId) use ($workflow) {
                    $workflow->signal('setSessionId', $sessionId);
                },
                new TemporalSignalLogger($workflow)
            ))->runCommand(
                $req->getCredentials()->unpack(),
                $req->getCommand()->unpack(),
                iterator_to_array($req->getFeatures()),
            );
        });

        $res = await($promise);
        var_export('run cmd done' . $info->workflowExecution->getID());

        $driverResponse = new DriverResponse();
        if ($res !== null) {
            $anyResponse = new Any();
            $anyResponse->pack($res);
            $driverResponse->setResponse($anyResponse);
        }
        return $driverResponse;
    }

    /**
     * @param string ...$arg
     */
    private function log(string $message, ...$arg): void
    {
        $this->logger->debug(sprintf($message, ...$arg));
    }
}
