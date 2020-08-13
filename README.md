# Neos CMS Elasticsearch indexer based on a job queue

[![Latest Stable Version](https://poser.pugx.org/flowpack/elasticsearch-contentRepositoryQueueIndexer/v/stable)](https://packagist.org/packages/flowpack/elasticsearch-contentRepositoryQueueIndexer) [![Total Downloads](https://poser.pugx.org/flowpack/elasticsearch-contentRepositoryQueueIndexer/downloads)](https://packagist.org/packages/flowpack/elasticsearch-contentRepositoryQueueIndexer)

This package can be used to index a huge amount of nodes in Elasticsearch indexes. This
package use the Flowpack JobQueue packages to handle the indexing asynchronously.

**Topics**

* [Installation](#installation-and-configuration)
* [Indexing](#indexing)
* [SupervisorD configuration](#supervisord-configuration)
* [Update Instructions](#update-instructions)


# Installation and Configuration

You need to install the correct Queue package based on your needs.

Available packages:

  - [sqlite](https://packagist.org/packages/flownative/jobqueue-sqlite)
  - [beanstalkd](https://packagist.org/packages/flowpack/jobqueue-beanstalkd)
  - [doctrine](https://packagist.org/packages/flowpack/jobqueue-doctrine)
  - [redis](https://packagist.org/packages/flowpack/jobqueue-redis)

Please check the package documentation for specific configurations.

The default configuration uses the FakeQueue, which is provided by the JobQueue.Common package. Note that with that package jobs are executed synchronous with the `flow nodeindexqueue:build` command.

Check the ```Settings.yaml``` to adapt based on the Queue package, you need to adapt the ```className```:

    Flowpack:
      JobQueue:
        Common:
          presets:
            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer':
              className: 'Flowpack\JobQueue\Common\Queue\FakeQueue'

If you use the [doctrine](https://packagist.org/packages/flownative/jobqueue-doctrine) package you have to set the ```tableName``` manually:

    Flowpack:
      JobQueue:
        Common:
          presets:
            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer':
              className: 'Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue'
          queues:
            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer':
              options:
                tableName: 'flowpack_jobqueue_QueueIndexer'
            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer.Live':
              options:
                tableName: 'flowpack_jobqueue_QueueIndexerLive'

# Indexing

## Batch Indexing

### How to build indexing jobs

    flow nodeindexqueue:build --workspace live

#### How to process indexing jobs

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue batch

## Live Indexing

You can disable async live indexing by editing ```Settings.yaml```:

    Flowpack:
      ElasticSearch:
        ContentRepositoryQueueIndexer:
          enableLiveAsyncIndexing: false

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue live

# Supervisord configuration

You can use tools like ```supervisord``` to manage long running processes. Bellow you can find a basic configuration:

    [supervisord]

    [supervisorctl]

    [program:elasticsearch_batch_indexing]
    command=php flow nodeindexqueue:work --queue batch
    stdout_logfile=AUTO
    stderr_logfile=AUTO
    numprocs=4
    process_name=elasticsearch_batch_indexing_%(process_num)02d
    environment=FLOW_CONTEXT="Production"
    autostart=true
    autorestart=true
    stopsignal=QUIT

    [program:elasticsearch_live_indexing]
    command=php flow nodeindexqueue:work --queue live
    stdout_logfile=AUTO
    stderr_logfile=AUTO
    numprocs=4
    process_name=elasticsearch_live_indexing_%(process_num)02d
    environment=FLOW_CONTEXT="Production"
    autostart=true
    autorestart=true
    stopsignal=QUIT

# Update Instructions

## Breaking change after an upgrade to 3.0

* Previously the Beanstalk queue package was installed by default, this is no longer
the case.

## Breaking change after an upgrade to 5.0

* The beanstalk queue configuration is removed. The FakeQueue is used if not configured to another queuing package.

License
-------

Licensed under MIT, see [LICENSE](LICENSE)
