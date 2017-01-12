# Neos CMS ElasticSearch indexer based on beanstalkd (job queue)

This package can be used to index a huge amount of nodes in ElasticSearch indexes. This 
package use Beanstalkd and the JobQueue package to handle ES indexing asynchronously.

How to build indexing job
-------------------------

    flow nodeindexqueue:build --workspace live
    
How to process indexing job
---------------------------

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work

You can use tools like ```supervisord``` to manage long runing process. Bellow you can
found a basic configuration:

    [supervisord]
   
    [supervisorctl]
   
    [program:elasticsearch_indexing]
    command=php flow job:work --queue Flowpack.ElasticSearch.ContentRepositoryQueueIndexer --limit 5000
    stdout_logfile=AUTO
    stderr_logfile=AUTO
    numprocs=12
    process_name=elasticsearch_indexing_%(process_num)02d
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
