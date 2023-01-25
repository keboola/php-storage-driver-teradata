<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

use Aws\S3\S3Client;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ABSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use LogicException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;

trait StorageTrait
{
    use StorageABSTrait;
    use StorageS3Trait;

    /**
     * @return StorageType::STORAGE_*
     */
    public function getStorageType(): string
    {
        return (string) getenv('STORAGE_TYPE');
    }

    /**
     * @param TableImportFromFileCommand|TableExportToFileCommand $cmd
     */
    public function setFilePathAndCredentials(Message $cmd, string $path, string $filename = ''): void
    {
        switch ($this->getStorageType()) {
            case StorageType::STORAGE_S3:
                $cmd->setFileProvider(FileProvider::S3);

                $cmd->setFilePath(
                    (new FilePath())
                        ->setRoot((string) getenv('AWS_S3_BUCKET'))
                        ->setPath($path)
                        ->setFileName(
                            $this->replaceManifestPrefix($filename, 'S3.')
                        )
                );

                $credentials = new Any();
                $credentials->pack(
                    (new S3Credentials())
                        ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                        ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                        ->setRegion((string) getenv('AWS_REGION'))
                );
                $cmd->setFileCredentials($credentials);
                break;
            case StorageType::STORAGE_ABS:
                $cmd->setFileProvider(FileProvider::ABS);

                $cmd->setFilePath(
                    (new FilePath())
                        ->setRoot((string) getenv('ABS_CONTAINER_NAME'))
                        ->setPath($path)
                        ->setFileName(
                            $this->replaceManifestPrefix($filename, 'ABS.')
                        )
                );

                $credentials = new Any();
                $credentials->pack(
                    (new ABSCredentials())
                        ->setAccountName((string) getenv('ABS_ACCOUNT_NAME'))
                        ->setAccountKey((string) getenv('ABS_ACCOUNT_KEY'))
                        // TODO generate it
                        ->setSasToken((string) getenv('ABS_SAS_TOKEN'))
                );
                $cmd->setFileCredentials($credentials);
                break;
            default:
                throw new LogicException(sprintf('Unknown STORAGE_TYPE "%s".', $this->getStorageType()));
        }
    }

    private function replaceManifestPrefix(string $filePath, string $prefix): string
    {
        return str_replace('%MANIFEST_PREFIX%', $prefix, $filePath);
    }

    public function clearStorageDir(string $dir): void
    {
        $client = $this->getStorageClient();

        switch ($this->getStorageType()) {
            case StorageType::STORAGE_S3:
                /** @var S3Client $client */
                $this->clearS3BucketDir(
                    $client,
                    (string) getenv('AWS_S3_BUCKET'),
                    $dir,
                );
                break;
            case StorageType::STORAGE_ABS:
                /** @var BlobRestProxy $client */
                $this->clearAbsContainerDir(
                    $client,
                    (string) getenv('ABS_CONTAINER_NAME'),
                    $dir,
                );
                break;
            default:
                throw new LogicException(sprintf('Unknown STORAGE_TYPE "%s".', $this->getStorageType()));
        }
    }

    /**
     * @param string $dir
     * @return array<int, array<string, mixed>>|ListBlobsResult
     */
    public function listStorageDirFiles(string $dir)
    {
        $client = $this->getStorageClient();

        switch ($this->getStorageType()) {
            case StorageType::STORAGE_S3:
                /** @var S3Client $client */
                return $this->listS3BucketDirFiles(
                    $client,
                    (string) getenv('AWS_S3_BUCKET'),
                    $dir,
                );
            case StorageType::STORAGE_ABS:
                /** @var BlobRestProxy $client */
                return $this->listAbsContainerDirFiles(
                    $client,
                    (string) getenv('ABS_CONTAINER_NAME'),
                    $dir,
                );
            default:
                throw new LogicException(sprintf('Unknown STORAGE_TYPE "%s".', $this->getStorageType()));
        }
    }

    /**
     * @return S3Client|BlobRestProxy
     */
    public function getStorageClient()
    {
        switch ($this->getStorageType()) {
            case StorageType::STORAGE_S3:
                return $this->getS3Client(
                    (string) getenv('AWS_ACCESS_KEY_ID'),
                    (string) getenv('AWS_SECRET_ACCESS_KEY'),
                    (string) getenv('AWS_REGION'),
                );
            case StorageType::STORAGE_ABS:
                return $this->getAbsClient(
                    (string) getenv('ABS_ACCOUNT_NAME'),
                    (string) getenv('ABS_ACCOUNT_KEY'),
                );
            default:
                throw new LogicException(sprintf('Unknown STORAGE_TYPE "%s".', $this->getStorageType()));
        }
    }

    public function getStorageFileAsCsvArray(string $fileName): array
    {
        $client = $this->getStorageClient();

        switch ($this->getStorageType()) {
            case StorageType::STORAGE_S3:
                $body = $this->getS3ObjectContent(
                    $client,
                    (string) getenv('AWS_S3_BUCKET'),
                    $fileName,
                );
                break;
            case StorageType::STORAGE_ABS:
                $body = $this->getAbsBlobContent(
                    $client,
                    (string) getenv('ABS_CONTAINER_NAME'),
                    $fileName,
                );
                break;
            default:
                throw new LogicException(sprintf('Unknown STORAGE_TYPE "%s".', $this->getStorageType()));
        }

        // parse as array
        $csvData = array_map('str_getcsv', explode(PHP_EOL, $body));
        array_pop($csvData);
        return $csvData;
    }

}
