<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

use DateTime;
use Keboola\FileStorage\Abs\ClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use UnexpectedValueException;

trait StorageABSTrait
{
    public function clearAbsContainerDir(BlobRestProxy $client, string $container, string $dir): void
    {
        $blobs = $this->listAbsContainerDirFiles($client, $container, $dir);

        foreach ($blobs->getBlobs() as $blob) {
            $client->deleteBlob($container, $blob->getName());
        }
    }

    public function listAbsContainerDirFiles(BlobRestProxy $client, string $container, string $dir): ListBlobsResult
    {
        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($dir);
        return $client->listBlobs($container, $listOptions);
    }

    public function getAbsClient(string $name, string $key): BlobRestProxy
    {
        $connectionString = sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            $name,
            $key,
        );
        return ClientFactory::createClientFromConnectionString(
            $connectionString
        );
    }

    private function getAbsBlobContent(BlobRestProxy $client, string $container, string $blob): string
    {
        $stream = $client
            ->getBlob($container, $blob)
            ->getContentStream();

        $content = stream_get_contents($stream);
        if ($content === false) {
            throw new UnexpectedValueException('ABS blob content cannot be loaded');
        }
        return $content;
    }

    protected function getAbsCredentialsForContainer(
        string $accountName,
        string $accountKey,
        string $container,
        string $permissions = 'rwl'
    ): string {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            $accountName,
            $accountKey,
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            $permissions,
            $expirationDate,
            (new DateTime())
        );
    }
}
