<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\Flow\Annotations as Flow;
use Neos\ContentRepository\Domain\Model\NodeInterface;

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
     */
    public function execute(QueueInterface $queue, Message $message)
    {
        $this->nodeIndexer->withBulkProcessing(function () {
            $numberOfNodes = count($this->nodes);
            $startTime = microtime(true);
            foreach ($this->nodes as $node) {
                /** @var NodeData $nodeData */
                $nodeData = $this->nodeDataRepository->findByIdentifier($node['nodeIdentifier']);

                // Skip this iteration if the nodedata can not be fetched (deleted node)
                if (!$nodeData instanceof NodeData) {
                    $this->log(sprintf('action=indexing step=failed node=%s message="Node data could not be loaded"', $node['nodeIdentifier']), \LOG_ERR);
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
                    $this->log(sprintf('action=indexing step=failed node=%s message="Node could not be processed"', $node['nodeIdentifier']));
                    continue;
                }

                $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
                $this->log(sprintf('action=indexing step=started node=%s', $currentNode->getIdentifier()));

                $this->nodeIndexer->indexNode($currentNode);
            }

            $this->nodeIndexer->flush();
            $duration = microtime(true) - $startTime;
            $rate = $numberOfNodes / $duration;
            $this->log(sprintf('action=indexing step=finished number_of_nodes=%d duration=%f nodes_per_second=%f', $numberOfNodes, $duration, $rate));
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
        return sprintf('Elasticsearch Indexing Job (%s)', $this->getIdentifier());
    }
}
