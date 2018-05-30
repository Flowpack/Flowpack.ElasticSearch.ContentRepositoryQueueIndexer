<?php
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

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;

/**
 * Elasticsearch Node Removal Job
 */
class RemovalJob extends AbstractIndexingJob
{
    /**
     * Execute the job removal of nodes.
     *
     * @param QueueInterface $queue
     * @param Message $message The original message
     * @return boolean TRUE if the job was executed successfully and the message should be finished
     * @throws \Exception
     */
    public function execute(QueueInterface $queue, Message $message)
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
                        $this->log(sprintf('action=removal step=failed node=%s message="Node data could not be loaded or faked"', $node['identifier']), \LOG_CRIT);
                        $this->_logger->logException($exception);
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
                    $this->log(sprintf('action=removal step=failed node=%s message="Node could not be processed"', $node['identifier']));
                    continue;
                }

                $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
                $this->log(sprintf('action=removal step=started node=%s', $currentNode->getIdentifier()));

                $this->nodeIndexer->removeNode($currentNode, $this->targetWorkspaceName);
            }

            $this->nodeIndexer->flush();
            $duration = microtime(true) - $startTime;
            $rate = $numberOfNodes / $duration;
            $this->log(sprintf('action=removal step=finished number_of_nodes=%d duration=%f nodes_per_second=%f', $numberOfNodes, $duration, $rate));
        });

        return true;
    }

    /**
     * Get a readable label for the job
     *
     * @return string A label for the job
     */
    public function getLabel()
    {
        return sprintf('Elasticsearch Removal Job (%s)', $this->getIdentifier());
    }
}
