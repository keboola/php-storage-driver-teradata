<?php

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

use Aws\Exception\AwsException;
use Aws\S3\S3Client;
use UnexpectedValueException;

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

    private function getS3ObjectContent(S3Client $s3Client, string $bucket, string $key): string
    {
        try {
            /** @var array{Body: resource} $file */
            $file = $s3Client->getObject([
                'Bucket' => $bucket,
                'Key' => $key,
            ]);
        } catch (AwsException $e) {
            throw new UnexpectedValueException(sprintf('S3 object content cannot be loaded: %s', $e->getMessage()));
        }

        return (string) $file['Body'];
    }
}
