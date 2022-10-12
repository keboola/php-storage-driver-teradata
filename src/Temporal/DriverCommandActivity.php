<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Temporal;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Driver\DriverCommandActivityInterface;
use Keboola\StorageDriver\Shared\Utils\StdErrLogger;
use Keboola\StorageDriver\Shared\Utils\TemporalSignalLogger;
use Keboola\StorageDriver\Teradata\AsyncTask;
use Keboola\StorageDriver\Teradata\TeradataDriverClient;
use React\EventLoop\Loop;
use Spatie\Async\Pool;
use Spatie\Async\Process\ParallelProcess;
use Spatie\Async\Runtime\ParentRuntime;
use Temporal\Activity;
use Temporal\Internal\Activity\ActivityContext;

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
        /** @var ActivityContext $ctx */
        $ctx = Activity::getCurrentContext();
        assert($info->workflowExecution !== null);
        $this->log("workflowId=" . $info->workflowExecution->getID());
        $this->log("runId=" . $info->workflowExecution->getRunID());
        $this->log("activityId=" . $info->id);
        $this->log("activityDeadline=" . $info->deadline->format(\DateTimeInterface::ATOM));

        $heartbeat = Loop::addPeriodicTimer(0.1, function () use (&$ctx): void {
            $ctx->heartbeat('running');
        });

        $pool = Pool::create();
        $pool->autoload(__DIR__ . '/../../service/driver/thread.php');
        $process = ParentRuntime::createProcess(
            new AsyncTask($req, $info->workflowExecution->getID())
        );
        $driver = $pool->add($process)
            ->then(function (?Message $res) use ($heartbeat) {
                //Loop::cancelTimer($heartbeat);
                return $res;
            })->catch(function (\Throwable $e) use ($heartbeat) {
                //Loop::cancelTimer($heartbeat);
                throw $e;
            });
        Loop::run();
        $pool->wait();

        $res = $driver->getOutput();
        $driverResponse = new DriverResponse();
        if ($res !== null) {
            $anyResponse = new Any();
            $anyResponse->pack($res);
            $driverResponse->setResponse($anyResponse);
        }
        Loop::stop();
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
