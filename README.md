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

You can configure Ignite from the Config.groovy file (with limited configuration support for now):

```
ignite {
    enabled=true
    gridName="myGrid"
    webSessionClusteringEnabled=true
    peerClassLoadingEnabled=false
    discoverySpi {
        networkTimeout = 5000
        addresses = ["${myIP}:47500..47509"]
    }
}
```
	
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
