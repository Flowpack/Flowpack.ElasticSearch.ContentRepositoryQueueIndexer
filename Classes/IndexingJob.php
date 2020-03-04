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

use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Elasticsearch Node Indexing Job
 */
class IndexingJob extends AbstractIndexingJob
{
    /**
     * Execute the indexing of nodes
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
                    $this->logger->notice(sprintf('Node data of node %s could not be loaded. Node might be deleted."', $node['identifier']), LogEnvironment::fromMethodName(__METHOD__));
                    continue;
                }

                $context = $this->contextFactory->create([
                    'workspaceName' => $this->targetWorkspaceName ?: $nodeData->getWorkspace()->getName(),
                    'invisibleContentShown' => true,
                    'inaccessibleContentShown' => false,
                    'dimensions' => $node['dimensions']
                ]);
                $currentNode = $this->nodeFactory->createFromNodeData($nodeData, $context);

                // Skip this iteration if the node can not be fetched from the current context
                if (!$currentNode instanceof NodeInterface) {
                    $this->logger->warning(sprintf('Node %s could not be processed"', $node['identifier']), LogEnvironment::fromMethodName(__METHOD__));
                    continue;
                }

                $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
                $this->nodeIndexer->indexNode($currentNode, $this->targetWorkspaceName);
            }

            $this->nodeIndexer->flush();
            $duration = microtime(true) - $startTime;
            $rate = $numberOfNodes / $duration;
            $this->logger->info(sprintf('Indexed %s nodes in %s seconds (%s nodes per second)', $numberOfNodes, $duration, $rate), LogEnvironment::fromMethodName(__METHOD__));
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
        return sprintf('Elasticsearch Indexing Job (%s)', $this->getIdentifier());
    }
}
