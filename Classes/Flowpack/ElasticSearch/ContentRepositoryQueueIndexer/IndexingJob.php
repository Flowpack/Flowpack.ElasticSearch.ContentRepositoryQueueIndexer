<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

/*                                                                                                       *
 * This script belongs to the TYPO3 Flow package "Flowpack.ElasticSearch.ContentRepositoryQueueIndexer". *
 *                                                                                                       *
 * It is free software; you can redistribute it and/or modify it under                                   *
 * the terms of the GNU Lesser General Public License, either version 3                                  *
 *  of the License, or (at your option) any later version.                                               *
 *                                                                                                       *
 * The TYPO3 project - inspiring people to share!                                                        *
 *                                                                                                       */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Jobqueue\Common\Job\JobInterface;
use TYPO3\Jobqueue\Common\Queue\Message;
use TYPO3\Jobqueue\Common\Queue\QueueInterface;
use TYPO3\TYPO3CR\Domain\Factory\NodeFactory;
use TYPO3\TYPO3CR\Domain\Model\NodeData;
use TYPO3\TYPO3CR\Domain\Repository\NodeDataRepository;
use TYPO3\TYPO3CR\Domain\Service\ContextFactory;

/**
 * ElasticSearch Indexing Job Interface
 */
class IndexingJob implements JobInterface {

	/**
	 * @var string
	 */
	protected $indexPostfix;

	/**
	 * @var string
	 */
	protected $nodeIdentifier;

	/**
	 * @var string
	 */
	protected $workspaceName;

	/**
	 * @var string
	 */
	protected $dimensions;

	/**
	 * @Flow\Inject
	 * @var NodeIndexer
	 */
	protected $nodeIndexer;

	/**
	 * @Flow\Inject
	 * @var NodeDataRepository
	 */
	protected $nodeDataRepository;

	/**
	 * @Flow\Inject
	 * @var NodeFactory
	 */
	protected $nodeFactory;

	/**
	 * @Flow\Inject
	 * @var ContextFactory
	 */
	protected $contextFactory;

	/**
	 * @Flow\Inject
	 * @var LoggerInterface
	 */
	protected $logger;

	/**
	 * @param string $indexPostfix
	 * @param string $nodeIdentifier
	 * @param string $workspaceName
	 * @param array $dimensions
	 */
	public function __construct($indexPostfix, $nodeIdentifier, $workspaceName, array $dimensions = array()) {
		$this->indexPostfix = $indexPostfix;
		$this->nodeIdentifier = $nodeIdentifier;
		$this->workspaceName = $workspaceName;
		$this->dimensions = $dimensions;
	}

	/**
	 * Execute the job
	 * A job should finish itself after successful execution using the queue methods.
	 *
	 * @param QueueInterface $queue
	 * @param Message $message The original message
	 * @return boolean TRUE if the job was executed successfully and the message should be finished
	 */
	public function execute(QueueInterface $queue, Message $message) {
		/** @var NodeData $nodeData */
		$nodeData = $this->nodeDataRepository->findByIdentifier($this->nodeIdentifier);
		$context = $this->contextFactory->create([
			'workspaceName' => $this->workspaceName,
			'dimensions' => $this->dimensions
		]);
		$currentNode = $this->nodeFactory->createFromNodeData($nodeData, $context);
		$this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
		$this->nodeIndexer->indexNode($currentNode);
		$this->nodeIndexer->flush();

		return TRUE;
	}

	/**
	 * Get an optional identifier for the job
	 *
	 * @return string A job identifier
	 */
	public function getIdentifier() {
		return $this->nodeIdentifier;
	}

	/**
	 * Get a readable label for the job
	 *
	 * @return string A label for the job
	 */
	public function getLabel() {
		return sprintf('ElasticSearch Indexing Job (%s)', $this->getIdentifier());
	}

}
