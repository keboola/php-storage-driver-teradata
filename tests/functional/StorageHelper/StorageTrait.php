<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ABSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use LogicException;

trait StorageTrait
{
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
}
