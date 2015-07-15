import grails.plugin.webxml.FilterManager
import org.apache.ignite.IgniteCheckedException
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.apache.log4j.Logger
import org.grails.ignite.DistributedSchedulerServiceImpl
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.NoSuchBeanDefinitionException

class IgniteGrailsPlugin {
    // the plugin version
    def version = "0.3.0-SNAPSHOT"
    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "2.3 > *"
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
            "grails-app/views/error.gsp"
    ]

    def title = "Grails Ignite Plugin" // Headline display name of the plugin
    def author = "Dan Stieglitz"
    def authorEmail = "dstieglitz@stainlesscode.com"
    def description = '''\
A plugin for the Apache Ignite data grid framework.
'''

    // URL to the plugin's documentation
    def documentation = "http://grails.org/plugin/ignite"

    // Extra (optional) plugin metadata

    // License: one of 'APACHE', 'GPL2', 'GPL3'
    def license = "APACHE"

    // Details of company behind the plugin (if there is one)
//    def organization = [ name: "My Company", url: "http://www.my-company.com/" ]

    // Any additional developers beyond the author specified above.
    def developers = [[name: "Dan Stieglitz", email: "dstieglitz@stainlesscode.com"]]

    // Location of the plugin's issue tracker.
    def issueManagement = [system: "GITHUB", url: "https://github.com/dstieglitz/grails-ignite/issues"]

    // Online location of the plugin's browseable source code.
    def scm = [url: "https://github.com/dstieglitz/grails-ignite"]

    def loadAfter = ['logging']

    static def IGNITE_WEB_SESSION_CACHE_NAME = 'session-cache'
    static def DEFAULT_GRID_NAME = 'grid'

//    def LOG = LoggerFactory.getLogger('grails.plugin.ignite.IgniteGrailsPlugin')

    def getWebXmlFilterOrder() {
        [IgniteWebSessionsFilter: FilterManager.CHAR_ENCODING_POSITION + 100]
    }

    def doWithWebDescriptor = { xml ->
        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        def configuredGridName = DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        // FIXME no log.(anything) output from here
        println "Web session clustering enabled in config? ${webSessionClusteringEnabled}"

        if (webSessionClusteringEnabled) {
//            def listenerNode = xml.'listener'
//            listenerNode[listenerNode.size() - 1] + {
//                listener {
//                    'listener-class'('org.apache.ignite.startup.servlet.ServletContextListenerStartup')
//                }
//            }

            println "Configuring WebSessionFilter for grid ${configuredGridName}"
            def contextParam = xml.'context-param'
            contextParam[contextParam.size() - 1] + {
                'filter' {
                    'filter-name'('IgniteWebSessionsFilter')
                    'filter-class'('org.apache.ignite.cache.websession.WebSessionFilter')
                    'init-param' {
                        'param-name'('IgniteWebSessionsGridName')
                        'param-value'(configuredGridName)
                    }
                }
            }

            def filterMappingNode = xml.'filter-mapping'
            filterMappingNode[filterMappingNode.size() - 1] + {
                'filter-mapping' {
                    'filter-name'('IgniteWebSessionsFilter')
                    'url-pattern'('/*')
                }
            }

            contextParam[contextParam.size() - 1] + {
                'context-param' {
                    'param-name'('IgniteWebSessionsCacheName')
                    'param-value'(IGNITE_WEB_SESSION_CACHE_NAME)
                }
            }
        }
    }

    def doWithSpring = {
        def peerClassLoadingEnabledInConfig = (!(application.config.ignite.peerClassLoadingEnabled instanceof ConfigObject)
                && application.config.ignite.peerClassLoadingEnabled.equals(true))

        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        def configuredGridName = DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        println "configured grid ${configuredGridName}"

        def configuredNetworkTimeout = 3000
        if (!(application.config.ignite.discoverySpi.networkTimeout instanceof ConfigObject)) {
            configuredNetworkTimeout = application.config.ignite.discoverySpi.networkTimeout
        }

        def configuredAddresses = []
        if (!(application.config.ignite.discoverySpi.addresses instanceof ConfigObject)) {
            configuredAddresses = application.config.ignite.discoverySpi.addresses
        }

        /*
         * Only configure Ignite if the configuration value ignite.enabled=true is defined
         */
        if (!(application.config.ignite.enabled instanceof ConfigObject) && application.config.ignite.enabled.equals(true)) {
            // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
            //gridLogger(Log4JLogger)

            igniteCfg(IgniteConfiguration) {
                gridName = configuredGridName
                peerClassLoadingEnabled = peerClassLoadingEnabledInConfig

                marshaller = { OptimizedMarshaller marshaller ->
                    requireSerializable = false
                }

                //            marshaller = { JdkMarshaller marshaller ->
                ////                requireSerializable = false
                //            }

                //            marshaller = { GroovyOptimizedMarshallerDecorator dec ->
                //                underlyingMarshaller = { OptimizedMarshaller mar ->
                //                    requireSerializable = false
                //                }
                //            }

                if (webSessionClusteringEnabled) {
                    cacheConfiguration = { CacheConfiguration cacheConfiguration ->
                        name = IGNITE_WEB_SESSION_CACHE_NAME
                        cacheMode = CacheMode.PARTITIONED
                        backups = 1
                        evictionPolicy = { LruEvictionPolicy lruEvictionPolicy ->
                            maxSize = 10000
                        }
                    }
                }

                includeEventTypes = [org.apache.ignite.events.EventType.EVT_TASK_STARTED,
                        org.apache.ignite.events.EventType.EVT_TASK_FINISHED,
                        org.apache.ignite.events.EventType.EVT_TASK_FAILED,
                        org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT,
                        org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET,
                        org.apache.ignite.events.EventType.EVT_TASK_REDUCED,
                        org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT,
                        org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ,
                        org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ]

                discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
                    networkTimeout = configuredNetworkTimeout
                    ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder ->
                        addresses = configuredAddresses
                    }
                }

//                deploymentSpi = { LowcalDeploymentSpi impl ->
//
//                }

//                serviceConfiguration = [{ ServiceConfiguration serviceConfiguration ->
//                    name = "distributedSchedulerService"
//                    maxPerNodeCount = 1
//                    totalCount = 1
//                    service = { DistributedSchedulerServiceImpl impl -> }
//                }]

                //gridLogger = ref('gridLogger')
            }

            grid(org.grails.ignite.DeferredStartIgniteSpringBean) { bean ->
                bean.lazyInit = true
                bean.dependsOn = ['persistenceInterceptor']
                configuration = ref('igniteCfg')
            }
        }
    }

    def doWithDynamicMethods = { ctx ->
        // TODO Implement registering dynamic methods to classes (optional)
    }

    def doWithApplicationContext = { ctx ->
        System.setProperty("IGNITE_QUIET", "false");
        //
        // FIXME need to start grid LAST, can't configure with spring this way. Maybe wrap in a delayed start
        // bean or something
        //
//        def configuredAddresses = []
//        if (!(application.config.ignite.discoverySpi.addresses instanceof ConfigObject)) {
//            configuredAddresses = application.config.ignite.discoverySpi.addresses
//        }
//
//        IgniteConfiguration config = new IgniteConfiguration();
//        config.setMarshaller(new OptimizedMarshaller(false));
//        def discoverySpi = new org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi();
//        discoverySpi.setNetworkTimeout(5000);
//        def ipFinder = new org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder();
//        ipFinder.setAddresses(configuredAddresses)
//        discoverySpi.setIpFinder(ipFinder)
//        def grid = Ignition.start(config);

        try {
            def grid = ctx.getBean('grid')
            // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
//            grid.configuration().setGridLogger(new Log4JLogger())
            log.info "Starting Ignite grid..."
            grid.start()
            grid.services().deployClusterSingleton("distributedSchedulerService", new DistributedSchedulerServiceImpl());
        } catch (NoSuchBeanDefinitionException e) {
            log.warn e.message
        } catch (IgniteCheckedException e) {
            log.error e.message, e
        }

//        ctx.getBean('distributedSchedulerService').grid = grid
    }

    def onChange = { event ->
        // TODO Implement code that is executed when any artefact that this plugin is
        // watching is modified and reloaded. The event contains: event.source,
        // event.application, event.manager, event.ctx, and event.plugin.
    }

    def onConfigChange = { event ->
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }

    def onShutdown = { event ->
        // TODO Implement code that is executed when the application shuts down (optional)
    }
}
