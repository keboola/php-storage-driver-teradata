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
        $storage = $factory->select('driver');
        $promise = async(function () use (&$ctx, &$manager, $workflowId): int {
            async(function () use (&$ctx): void {
                while (true) {
                    $this->log('beat');
                    $ctx->heartbeat('running');
                    await(\React\Promise\Timer\sleep(1));
                }
            })();
            await(async(function () use ($workflowId, &$manager): void {
                while (true) {
                    $services = $manager->list();
                    $this->log('waiting '.implode(',',$services));
                    if (!array_key_exists($workflowId, $services)) {
                        break;
                    }
                    await(\React\Promise\Timer\sleep(1));
                }
            })());

            return 1;
        })();

        await($promise);
        $result = $storage->get($workflowId);

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
