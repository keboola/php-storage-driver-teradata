<?php

declare(strict_types=1);

use Google\Protobuf\Internal\GPBType;
use Keboola\StorageDriver\Teradata\Client as TeradataClient;

require __DIR__ . '/vendor/autoload.php';

$client = new TeradataClient('127.0.0.1:9001', [
    'credentials' => \Grpc\ChannelCredentials::createInsecure(),
]);

$req = new \Keboola\StorageDriver\Command\Common\DriverRequest();
$any = new \Google\Protobuf\Any();
$any->pack(new \Keboola\StorageDriver\Command\Backend\RemoveBackendCommand());
$req->setCommand($any);
$features = new \Google\Protobuf\Internal\RepeatedField(GPBType::STRING);
$req->setFeatures($features);
$any = new \Google\Protobuf\Any();
$any->pack(new Keboola\StorageDriver\Credentials\GenericBackendCredentials());
$req->setCredentials($any);

[$response, $status] = $client->RunCommand($req)->wait();

var_export($status);
echo PHP_EOL;
