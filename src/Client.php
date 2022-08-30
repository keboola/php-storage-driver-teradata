<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Keboola\StorageDriver\Command\Common\DriverRequest;

class Client extends \Grpc\BaseStub
{
    public function RunCommand(DriverRequest $request, $metadata = [], $options = []): \Grpc\UnaryCall
    {
        return $this->_simpleRequest(
            '/keboola.storageDriver.command.common.Driver/RunCommand',
            $request,
            [DriverRequest::class, 'decode'],
            $metadata,
            $options
        );
    }
}
