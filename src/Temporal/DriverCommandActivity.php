<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Temporal;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Driver\DriverCommandActivityInterface;
use Keboola\StorageDriver\Shared\Utils\StdErrLogger;
use Spiral\Goridge\RPC\RPC;
use Temporal\Activity;
use Temporal\Internal\Activity\ActivityContext;
use function React\Async\async;
use function React\Async\await;

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
        $workflowId = $info->workflowExecution->getID();
        $this->log("workflowId=" . $workflowId);
        $this->log("runId=" . $info->workflowExecution->getRunID());
        $this->log("activityId=" . $info->id);
        $this->log("activityDeadline=" . $info->deadline->format(\DateTimeInterface::ATOM));
        async(function () use (&$ctx): void {
            while (true) {
                $this->log('beat');
                $ctx->heartbeat('running');
                await(\React\Promise\Timer\sleep(1));
            }
        })();
        await(\React\Promise\Timer\sleep(10));

        $rpc = RPC::fromGlobals();
        $manager = new \Spiral\RoadRunner\Services\Manager($rpc);
        $result = $manager->create(
            $workflowId,
            'php /code/service/driver/driver.php ' . $workflowId,
            1, 0, false,
            [
                'ACTIVITY_INPUT' => $req->serializeToJsonString(),
                'WORKFLOW_ID' => $workflowId,
            ]
        );
        if (!$result) {
            throw new \Spiral\RoadRunner\Services\Exception\ServiceException('Service creation failed.');
        }

        $factory = new \Spiral\RoadRunner\KeyValue\Factory($rpc);
        $storage = $factory->select('storage-driver');
        $promise = async(function () use ($workflowId, &$storage, &$manager): void {
            while (true) {
                $val = $storage->get($workflowId);
                if (is_string($val) && $val !== '') {
                    break;
                }
                $this->log('waiting ' . implode(',', $manager->list()));
                await(\React\Promise\Timer\sleep(1));
            }
        })();

        await($promise);
        var_export('done ' . $workflowId);
        $result = $storage->get($workflowId);
        $result = json_decode($result, true, 512, JSON_THROW_ON_ERROR);
        if (array_key_exists('exception', $result)) {
            throw new \Exception(json_encode($result));
        }
        $result = $result['response'] ?? null;

        $driverResponse = new DriverResponse();
        if ($result !== null) {
            $anyResponse = new Any();
            $anyResponse->pack($result);
            $driverResponse->setResponse($anyResponse);
        }

        $storage->delete($workflowId);
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
