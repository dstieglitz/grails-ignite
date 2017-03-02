#Grails Ignite Plugin

A plugin that provides basic Grails integration with the Apache Ignite compute grid framework.

#Supported Features

* Grid creation via Spring DSL and injection into Grails artifacts
* Web session clustering (http://apacheignite.readme.io/docs/web-session-clustering)
* Distributed task management using a `DistributedSchedulerService`

#Under Development

* Clustered Hibernate L2 Caching


#Grid Bean

The plugin provides a configured instance of the Ingite grid as a bean called "grid", which you can access via injection in controllers and services:

	def grid


#Configuration

In order to support hibernate l2 caching, which requires the Ignite grid to be started prior to the sessionFactory and therefore the vast majority of Grails artifacts, Ignite must be configured from external configuration files. 

The external files must be referenced in the ignite configuration block in Config.groovy:

```
ignite {
    enabled=true
    config.locations = [
            "file:ignite/conf/*.groovy"
    ]
    gridName="myGrid"
    
    /*
     * This setting must be enabled on the machine that builds the WAR file for the target environment,
     * since it will determine if the correct filters are incorporated into the web.xml file
     */
    webSessionClusteringEnabled=true
    
    /*
     * Configure timeouts for long-running jobs. If not specified, defaults to 60000
     */
    defaultJobTimeout = 60000
    
    /**
     * Set of Ant paths to exclude from web session clustering logic
     */
    webSessionClusteringPathExcludes = ['*/api/**','*/otherExclude/']
    
    /** 
      * Enable distributed hibernate l2 caching
      * You must also set the region factory correctly 
      */
    l2CacheEnabled=true
    
    /**
     * DEFAULTS; you can also configure individual caches as spring beans 
     */
    l2Cache {
        entity {
            evictionPolicy = 'lru'
            cacheMode = CacheMode.PARTITIONED
            memoryMode = CacheMemoryMode.OFFHEAP_TIERED
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL
            writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC
            offHeapMaxMemory = (1024L * 1024L * 1024L) * 0.25;
            maxElements = 1000
            swapEnabled = false
            statisticsEnabled = true
            managementEnabled = true
        }
        association {
            evictionPolicy = 'lru'
            cacheMode = CacheMode.PARTITIONED
            memoryMode = CacheMemoryMode.OFFHEAP_TIERED
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL
            writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC
            offHeapMaxMemory = (1024L * 1024L * 1024L) * 0.25;
            maxElements = 1000
            swapEnabled = false
            statisticsEnabled = true
            managementEnabled = true
        }
    }
    
    peerClassLoadingEnabled=false
    discoverySpi {
        networkTimeout = 5000
        addresses = ["${myIP}:47500..47509"]
    }
}
```

The files can be located anywhere but in the example above we have put them under ignite/conf in the project root. In a production deployment they would likely be in a completely different directory.

The configuration files follow the standard Ignite spring configuration conventions, however they must (for the time being) be expressed as Grails Spring DSL files for use with a BeanBuilder.

See the `ignite/conf` directory for sample configuration files. For basic configuration you can copy the directory to your project.

#Startup

In order to facilitate the correct startup order of the Ignite grid (to support hibernate L2 caching it must start before the SessionFactory, which in turn is started by Grails before the application), Ignite uses a special startup hook called `IgniteStartupHelper`. The `IgniteStartupHelper` class will load it's own application context specifically for the Ignite grid. This application context can be managed using the Spring DSL by specifying beans in *Resources.groovy files in the `ignite/conf` directory of your application, or by specifying external config directories for these files:

```
ignite {
    enabled=true
    config.locations = [
            "file:ignite/conf/*.groovy"
    ]
    gridName="myGrid"
```

#Discovery

Ignite can be configured to discover nodes via static, multicast or s3 discovery mechanisms. You need to include the correct ignite spring resources in your project and set the proper Grails configuration.

See IgniteResources.groovy

The example below shows how to configure discovery via the Grails configuration:

```
    discoverySpi {
        s3Discovery = true 
        awsAccessKey = <ACCESS KEY WITH S3 PERMISSIONS>
        awsSecretKey = <SECRET KEY FOR ACCESS KEY>
        s3DiscoveryBucketName = "ignite-localdev"

		/* OR */
		
        multicastDiscovery = false
        networkTimeout = 5000
        addresses = ["${myIP}:47500..47509".toString()]

		/* OR */
		
        // use static IP discovery
        addresses = ['1.2.3.4','5.6.7.8']
    }
 ```

#Logging

The project contains an implementation of `IgniteLogger` for use with Grails. This class allows you to use the Grails log4j DSL to configure logging for the embedded Ignite node. The logger can be configured from the Ignite spring bean:

```
        gridLogger(org.grails.ignite.IgniteGrailsLogger)
        
        igniteCfg(IgniteConfiguration) {
        	gridLogger = ref('gridLogger')
        }
```


#Distributed Hibernate L2 Caching

*Requires Hibernate 4*

A basic functional version of distributed Hibernate L2 caching can be utilized by setting the region factory class as follows:

```
hibernate {
    cache.region.factory_class = 'org.grails.ignite.HibernateRegionFactory'
    org.apache.ignite.hibernate.grid_name = '<MY GRID NAME>'
    org.apache.ignite.hibernate.default_access_type = 'READ_ONLY' // see Ignite docs
}
```

By default, the plugin will create caches with reasonable defaults (whatever defaults existing when using Ignite.getOrCreateCache()) on demand when Hibernate configures the regions. You can override these defaults by creating the appropriate CacheConfiguration beans in `IgniteCacheResources.groovy`

##Example Spring Cache Configuration

In `grails-app/ignite/conf/resources/IgniteCacheResources.groovy`:

```
    'com.mypackage.MyDomainClass' { bean ->
        name = 'com.package.MyDomainClass'
        cacheMode = CacheMode.PARTITIONED
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL
        writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC
        evictionPolicy = { org.apache.ignite.cache.eviction.lru.LruEvictionPolicy policy ->
            maxSize = 1000000
        }
    }
```

See Also:

http://apacheignite.gridgain.org/v1.1/docs/evictions

	
#Scheduled, Distributed Tasks

This plugin provides an Ignite service called `DistributedSchedulerService` that provides a partial implementation of the `ScheduledThreadPoolExectutor` interface but allows you to run the submitted jobs on the Ignite grid. 

The methods `scheduleAtFixedRate` and `scheduleWithFixedDelay` are currently implemented. The service keeps track of submitted job schedules using a grid-aware Set that is configured for REPLICATED caching, so that if any grid node goes down.

A Grails service of the same name ("`DistributedSchedulerService`") is also provided to facilitiate easy injection into other Grails applications.

##Example
```
distributedSchedulerService.scheduleAtFixedRate(new HelloWorldGroovyTask(), 0, 1000, TimeUnit.MILLISECONDS);
```
	       
This example shows how to schedule the supplied task to execute once per second on the entire grid, regardless of the grid topology. The execution will be evenly load-balanced across all grid nodes. If any grid nodes go down the rebalancing will result in the same execution rate (once per second in this example).
	       
The example above can be run out-of-the-box (the `HelloWorldGroovyTask` is included in the plugin). You can then try neat things like spinning up another instance on a different port, and watching the grid fail-over and recover by killing one instance and bringing it back up.
	

#Notes

Requires h2 version 1.3.137 (or higher)? Make sure you do this:

    inherits("global") {
        // specify dependency exclusions here; for example, uncomment this to disable ehcache:
        // excludes 'ehcache'
        excludes 'h2'
    }
