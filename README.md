# Neos CMS ElasticSearch indexer based on beanstalkd (job queue)

This package can be used to index a huge amount of nodes in ElasticSearch indexes. This 
package use Beanstalkd and the JobQueue package to handle ES indexing asynchronously.

## Batch Indexing

### How to build indexing job

    flow nodeindexqueue:build --workspace live
    
### How to process indexing job

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue batch

## Live Indexing

You can enable async live indexing by editing ```Settings.yaml```:

    Flowpack:
      ElasticSearch:
        ContentRepositoryQueueIndexer:
          enableLiveAsyncIndexing: true
          
You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue live          

## Supervisord configuration

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
    command=php flow nodeindexqueue:live --queue live
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
