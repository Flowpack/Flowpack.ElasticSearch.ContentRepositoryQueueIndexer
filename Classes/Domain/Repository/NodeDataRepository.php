<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Internal\Hydration\IterableResult;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Service\NodeTypeIndexingConfiguration;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Repository;

/**
 * @Flow\Scope("singleton")
 */
class NodeDataRepository extends Repository
{
    public const ENTITY_CLASSNAME = NodeData::class;

    /**
     * @Flow\Inject
     * @var NodeTypeIndexingConfiguration
     */
    protected $nodeTypeIndexingConfiguration;

    /**
     * @Flow\Inject
     * @var EntityManagerInterface
     */
    protected $entityManager;

    /**
     * @param string $workspaceName
     * @param string|null $lastPersistenceObjectIdentifier
     * @param int $maxResults
     * @return IterableResult
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    public function findAllBySiteAndWorkspace(string $workspaceName, string $lastPersistenceObjectIdentifier = null, int $maxResults = 1000): IterableResult
    {
        $queryBuilder = $this->entityManager->createQueryBuilder();
        $queryBuilder->select('n.Persistence_Object_Identifier persistenceObjectIdentifier, n.identifier identifier, n.dimensionValues dimensions, n.nodeType nodeType, n.path path')
            ->from(NodeData::class, 'n')
            ->where('n.workspace = :workspace AND n.removed = :removed AND n.movedTo IS NULL')
            ->setMaxResults((integer)$maxResults)
            ->setParameters([
                ':workspace' => $workspaceName,
                ':removed' => false,
            ])
            ->orderBy('n.Persistence_Object_Identifier');

        if (!empty($lastPersistenceObjectIdentifier)) {
            $queryBuilder->andWhere($queryBuilder->expr()->gt('n.Persistence_Object_Identifier', $queryBuilder->expr()->literal($lastPersistenceObjectIdentifier)));
        }

        $excludedNodeTypes = array_keys(array_filter($this->nodeTypeIndexingConfiguration->getIndexableConfiguration(), static function ($value) {
            return !$value;
        }));

        if (!empty($excludedNodeTypes)) {
            $queryBuilder->andWhere($queryBuilder->expr()->notIn('n.nodeType', $excludedNodeTypes));
        }

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
            $iteration++;
        }
    }
}
