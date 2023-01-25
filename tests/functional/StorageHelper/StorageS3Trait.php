<?php

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

use Aws\S3\S3Client;

trait StorageS3Trait
{
    public function clearS3BucketDir(S3Client $client, string $bucket, string $dir): void
    {
        $client->deleteMatchingObjects(
            $bucket,
            $dir,
        );
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function listS3BucketDirFiles(S3Client $client, string $bucket, string $dir): array
    {
        $result = $client->listObjects([
            'Bucket' => $bucket,
            'Prefix' => $dir,
        ]);
        /** @var array<int, array<string, mixed>> $contents */
        $contents = $result->get('Contents');
        return $contents ?? [];
    }

    public function getS3Client(string $key, string $secret, string $region): S3Client
    {
        return new S3Client([
            'credentials' => [
                'key' => $key,
                'secret' => $secret,
            ],
            'region' => $region,
            'version' => '2006-03-01',
        ]);
    }
}
