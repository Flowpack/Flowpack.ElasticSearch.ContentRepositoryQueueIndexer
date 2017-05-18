# Neos CMS Elasticsearch indexer based on a job queue

This package can be used to index a huge amount of nodes in Elasticsearch indexes. This
package use the Flowpack JobQueue packages to handle the indexing asynchronously.

# Breaking change after an upgrade to 3.0

Previously the Beanstalk queue package was installed by default, this is no longer
the case.

# Install and configure your Queue package

You need to install the correct Queue package based on your needs.

Available packages:

  - [sqlite](https://packagist.org/packages/flownative/jobqueue-sqlite)
  - [beanstalkd](https://packagist.org/packages/flownative/jobqueue-beanstalkd)
  - [doctrine](https://packagist.org/packages/flownative/jobqueue-doctrine)
  - [redis](https://packagist.org/packages/flownative/jobqueue-redis)

Please check the package documentation for specific configurations.

The default configuration uses Beanstalkd, but you need to install it manually:

    composer require flowpack/jobqueue-beanstalkd

Check the ```Settings.yaml``` to adapt based on the Queue package, you need to adapt the ```className```:

    Flowpack:
      JobQueue:
        Common:
          queues:
            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer':
              className: 'Flowpack\JobQueue\Beanstalkd\Queue\BeanstalkdQueue'

            'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer.Live':
              className: 'Flowpack\JobQueue\Beanstalkd\Queue\BeanstalkdQueue'

# Batch Indexing

## How to build indexing job

    flow nodeindexqueue:build --workspace live

## How to process indexing job

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue batch

# Live Indexing

You can disable async live indexing by editing ```Settings.yaml```:

    Flowpack:
      ElasticSearch:
        ContentRepositoryQueueIndexer:
          enableLiveAsyncIndexing: false

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue live

# Supervisord configuration

You can use tools like ```supervisord``` to manage long runing process. Bellow you can
found a basic configuration:

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

Acknowledgments
---------------

Development sponsored by [ttree ltd - neos solution provider](http://ttree.ch).

We try our best to craft this package with a lots of love, we are open to
sponsoring, support request, ... just contact us.

License
-------

Licensed under MIT, see [LICENSE](LICENSE)
