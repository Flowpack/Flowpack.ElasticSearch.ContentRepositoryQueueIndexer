<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Neos\Flow\Annotations as Flow;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\ThrowableStorageInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Elasticsearch Node Removal Job
 */
class RemovalJob extends AbstractIndexingJob
{
    /**
     * @Flow\Inject
     * @var ThrowableStorageInterface
     */
    protected $throwableStorage;

    /**
     * Execute the job removal of nodes.
     *
     * @param QueueInterface $queue
     * @param Message $message The original message
     * @return boolean TRUE if the job was executed successfully and the message should be finished
     * @throws \Exception
     */
    public function execute(QueueInterface $queue, Message $message): bool
    {
        $this->nodeIndexer->withBulkProcessing(function () {
            $numberOfNodes = count($this->nodes);
            $startTime = microtime(true);
            foreach ($this->nodes as $node) {
                /** @var NodeData $nodeData */
                $nodeData = $this->nodeDataRepository->findByIdentifier($node['persistenceObjectIdentifier']);

                // Skip this iteration if the nodedata can not be fetched (deleted node)
                if (!$nodeData instanceof NodeData) {
                    try {
                        $nodeData = $this->fakeNodeDataFactory->createFromPayload($node);
                    } catch (Exception $exception) {
                        $message = $this->throwableStorage->logThrowable($exception);
                        $this->logger->error(sprintf('Node data of node %s could not be loaded or faked: %s"', $node['identifier'], $message), LogEnvironment::fromMethodName(__METHOD__));

                        continue;
                    }
                }

                $context = $this->contextFactory->create([
                    'workspaceName' => $this->targetWorkspaceName ?: $nodeData->getWorkspace()->getName(),
                    'invisibleContentShown' => true,
                    'removedContentShown' => true,
                    'inaccessibleContentShown' => false,
                    'dimensions' => $node['dimensions']
                ]);
                $currentNode = $this->nodeFactory->createFromNodeData($nodeData, $context);

                // Skip this iteration if the node can not be fetched from the current context
                if (!$currentNode instanceof NodeInterface) {
                    $this->logger->info(sprintf('Node %s could not be processed', $node['identifier']), LogEnvironment::fromMethodName(__METHOD__));
                    continue;
                }

                $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
                $this->logger->info(sprintf('Removed node %s', $currentNode->getIdentifier()), LogEnvironment::fromMethodName(__METHOD__));

                $this->nodeIndexer->removeNode($currentNode, $this->targetWorkspaceName);
            }

            $this->nodeIndexer->flush();
            $duration = microtime(true) - $startTime;
            $rate = $numberOfNodes / $duration;
            $this->logger->info(sprintf('Removed %s nodes in %s seconds (%s nodes per second)', $numberOfNodes, $duration, $rate), LogEnvironment::fromMethodName(__METHOD__));
        });

        return true;
    }

    /**
     * Get a readable label for the job
     *
     * @return string A label for the job
     */
    public function getLabel(): string
    {
        return sprintf('Elasticsearch Removal Job (%s)', $this->getIdentifier());
    }
}
