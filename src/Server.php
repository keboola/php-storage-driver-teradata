<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Common\DriverInterface;
use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Spiral\RoadRunner\GRPC;

class Server implements DriverInterface
{
    public function RunCommand(GRPC\ContextInterface $ctx, DriverRequest $in): DriverResponse
    {
        $res = (new TeradataDriverClient())->runCommand(
            $in->getCredentials()->unpack(),
            $in->getCommand()->unpack(),
            iterator_to_array($in->getFeatures())
        );

        $anyResponse = new Any();
        if ($res !== null) {
            $anyResponse->pack($res);
        }

        return (new DriverResponse())
            ->setResponse($anyResponse);
    }
}
