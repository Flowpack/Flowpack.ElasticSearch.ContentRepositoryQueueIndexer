<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository;

use Doctrine\Common\Persistence\ObjectManager;
use Doctrine\ORM\Internal\Hydration\IterableResult;
use Doctrine\ORM\QueryBuilder;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Repository;

/**
 * @Flow\Scope("singleton")
 */
class NodeDataRepository extends Repository
{
    const ENTITY_CLASSNAME = 'Neos\ContentRepository\Domain\Model\NodeData';

    /**
     * @Flow\Inject
     * @var ObjectManager
     */
    protected $entityManager;

    /**
     * @param string $workspaceName
     * @param integer $firstResult
     * @param integer $maxResults
     * @return IterableResult
     */
    public function findAllBySiteAndWorkspace($workspaceName, $firstResult = 0, $maxResults = 1000)
    {

        /** @var QueryBuilder $queryBuilder */
        $queryBuilder = $this->entityManager->createQueryBuilder();

        $queryBuilder->select('n.Persistence_Object_Identifier nodeIdentifier, n.dimensionValues dimensions, n.nodeType nodeType, n.path path')
            ->from('Neos\ContentRepository\Domain\Model\NodeData', 'n')
            ->where("n.workspace = :workspace AND n.removed = :removed AND n.movedTo IS NULL")
            ->setFirstResult((integer)$firstResult)
            ->setMaxResults((integer)$maxResults)
            ->setParameters([
                ':workspace' => $workspaceName,
                ':removed' => false,
            ]);

        return $queryBuilder->getQuery()->iterate();
    }

    /**
     * Iterator over an IterableResult and return a Generator
     *
     * This method is useful for batch processing huge result set as it clear the object
     * manager and detach the current object on each iteration.
     *
     * @param IterableResult $iterator
     * @param callable $callback
     * @return \Generator
     */
    public function iterate(IterableResult $iterator, callable $callback = null)
    {
        $iteration = 0;
        foreach ($iterator as $object) {
            $object = current($object);
            yield $object;
            if ($callback !== null) {
                call_user_func($callback, $iteration, $object);
            }
            ++$iteration;
        }
    }
}
