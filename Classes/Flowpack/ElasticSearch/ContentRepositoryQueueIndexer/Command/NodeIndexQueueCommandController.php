<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

use Doctrine\Common\Persistence\ObjectManager;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Command\NodeIndexCommandController;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Mapping\NodeTypeMappingBuilder;
use TYPO3\Flow\Configuration\ConfigurationManager;
use TYPO3\Flow\Core\Booting\Scripts;
use TYPO3\Flow\Exception;
use TYPO3\Flow\Persistence\PersistenceManagerInterface;
use TYPO3\Jobqueue\Common\Job\JobManager;
use TYPO3\TYPO3CR\Domain\Model\NodeData;
use TYPO3\TYPO3CR\Domain\Model\NodeInterface;
use TYPO3\TYPO3CR\Domain\Repository\WorkspaceRepository;
use TYPO3\TYPO3CR\Search\Indexer\NodeIndexingManager;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController {

	/**
	 * @Flow\Inject
	 * @var JobManager
	 */
	protected $jobManager;

	/**
	 * @Flow\Inject
	 * @var NodeTypeMappingBuilder
	 */
	protected $nodeTypeMappingBuilder;

	/**
	 * @Flow\Inject
	 * @var NodeDataRepository
	 */
	protected $nodeDataRepository;

	/**
	 * @Flow\Inject
	 * @var WorkspaceRepository
	 */
	protected $workspaceRepository;

	/**
	 * @Flow\Inject
	 * @var NodeIndexer
	 */
	protected $nodeIndexer;

	/**
	 * @Flow\Inject
	 * @var LoggerInterface
	 */
	protected $logger;

	/**
	 * @param string $workspace
	 */
	public function buildCommand($workspace = NULL) {
		$indexPostfix = time();
		$this->createNextIndex($indexPostfix);
		$this->updateMapping();


		$this->outputLine(sprintf('Indexing on %s ... ', $indexPostfix));

		if ($workspace === NULL) {
			foreach ($this->workspaceRepository->findAll() as $workspace) {
				$this->indexWorkspace($workspace->getName(), $indexPostfix);
			}
		} else {
			$this->indexWorkspace($workspace, $indexPostfix);
		}
	}

	/**
	 * @param string $indexPostfix
	 */
	public function updateAliasCommand($indexPostfix) {
		$this->nodeIndexer->setIndexNamePostfix($indexPostfix);
		$this->nodeIndexer->updateIndexAlias();
	}

	/**
	 * @param string $workspaceName
	 * @param string $indexPostfix
	 */
	protected function indexWorkspace($workspaceName, $indexPostfix) {
		$offset = 0;
		$batchSize = 100;
		while (TRUE) {
			$result = $this->nodeDataRepository->findAllBySiteAndWorkspace($workspaceName, $offset, $batchSize);
			if ($result === array()) {
				break;
			}
			$jobData = [];
			foreach ($result as $data) {
				$jobData[] = [
					'nodeIdentifier' => $data['nodeIdentifier'],
					'dimensions' => $data['dimensions']

				];
			}
			$indexingJob = new IndexingJob($indexPostfix, $workspaceName, $jobData);
			$this->jobManager->queue('Flowpack.ElasticSearch.ContentRepositoryQueueIndexer', $indexingJob);
			$this->output('.');
			$offset += $batchSize;
		}
	}

	/**
	 * Create next index
	 * @param string $indexPostfix
	 */
	protected function createNextIndex($indexPostfix) {
		$this->nodeIndexer->setIndexNamePostfix($indexPostfix);
		$this->nodeIndexer->getIndex()->create();
		$this->logger->log('Created index ' . $this->nodeIndexer->getIndexName(), LOG_INFO);
	}

	/**
	 * Update Index Mapping
	 */
	protected function updateMapping() {
		$nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
		foreach ($nodeTypeMappingCollection as $mapping) {
			/** @var \Flowpack\ElasticSearch\Domain\Model\Mapping $mapping */
			$mapping->apply();
		}
		$this->logger->log('Updated Mapping.', LOG_INFO);
	}


}
