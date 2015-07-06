<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository;

use Doctrine\Common\Persistence\ObjectManager;
use Doctrine\ORM\Query;
use Doctrine\ORM\QueryBuilder;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Persistence\Repository;
use TYPO3\TYPO3CR\Exception;

/**
 * @Flow\Scope("singleton")
 */
class NodeDataRepository extends Repository {

	const ENTITY_CLASSNAME = 'TYPO3\TYPO3CR\Domain\Model\NodeData';

	/**
	 * @Flow\Inject
	 * @var ObjectManager
	 */
	protected $entityManager;

	/**
	 * @param string $workspaceName
	 * @param integer $firstResult
	 * @param integer $maxResults
	 * @return array
	 */
	public function findAllBySiteAndWorkspace($workspaceName, $firstResult = 0, $maxResults = 1000) {

		/** @var QueryBuilder $queryBuilder */
		$queryBuilder = $this->entityManager->createQueryBuilder();

		$queryBuilder->select('n.Persistence_Object_Identifier nodeIdentifier, n.dimensionValues dimensions')
			->from('TYPO3\TYPO3CR\Domain\Model\NodeData', 'n')
			->where("n.workspace = :workspace AND n.removed = :removed AND n.movedTo IS NULL")
			->setFirstResult((integer)$firstResult)
			->setMaxResults((integer)$maxResults)
			->setParameters([
				':workspace' => $workspaceName,
				':removed' => FALSE,
			]);

		return $queryBuilder->getQuery()->getArrayResult();
	}

}
