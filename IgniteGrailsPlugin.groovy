import grails.plugin.webxml.FilterManager
import org.apache.ignite.IgniteCheckedException
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.grails.ignite.DistributedSchedulerServiceImpl
import org.grails.ignite.IgniteStartupHelper
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

    def dependsOn = ['hibernate4': '* > 4.3.8.1']

    def loadAfter = ['logging']

    def loadBefore = ['hibernate', 'hibernate4']

//    def LOG = LoggerFactory.getLogger('grails.plugin.ignite.IgniteGrailsPlugin')

    def getWebXmlFilterOrder() {
        [IgniteWebSessionsFilter: FilterManager.CHAR_ENCODING_POSITION + 100]
    }

    def doWithWebDescriptor = { xml ->
        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        def configuredGridName = IgniteStartupHelper.DEFAULT_GRID_NAME
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
                    'param-value'(IgniteStartupHelper.IGNITE_WEB_SESSION_CACHE_NAME)
                }
            }
        }
    }

    def doWithSpring = {
        def peerClassLoadingEnabledInConfig = (!(application.config.ignite.peerClassLoadingEnabled instanceof ConfigObject)
                && application.config.ignite.peerClassLoadingEnabled.equals(true))

        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        def configuredGridName = IgniteStartupHelper.DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        def configuredNetworkTimeout = 3000
        if (!(application.config.ignite.discoverySpi.networkTimeout instanceof ConfigObject)) {
            configuredNetworkTimeout = application.config.ignite.discoverySpi.networkTimeout
        }

        def configuredAddresses = []
        if (!(application.config.ignite.discoverySpi.addresses instanceof ConfigObject)) {
            configuredAddresses = application.config.ignite.discoverySpi.addresses
        }

        def igniteEnabled = (!(application.config.ignite.enabled instanceof ConfigObject)
                && application.config.ignite.enabled.equals(true))

        // FIXME externalize
        def l2CacheEnabled = true

        /*
         * Only configure Ignite if the configuration value ignite.enabled=true is defined
         */
        if (igniteEnabled) {
            // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
            //gridLogger(Log4JLogger)

            if (l2CacheEnabled) {
                // Hibernate L2 cache parent configurations
                atomicCache(CacheConfiguration) {
                    cacheMode = CacheMode.PARTITIONED
                    atomicityMode = CacheAtomicityMode.ATOMIC
                    writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC
                }

                transactionalCache(CacheConfiguration) {
                    cacheMode = CacheMode.PARTITIONED
                    atomicityMode = CacheAtomicityMode.TRANSACTIONAL
                    writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC
                }

                // FIXME allow external cache configuration for optimization on a class-by-class basis
                // Hibernate L2 cache parent configurations
                application.domainClasses.each { clazz ->
//                    println clazz.name
//                    println clazz.fullName
                    "${clazz.fullName}" { bean ->
                        // FIXME need to be able to configure transactional cache
                        bean.parent = ref('atomicCache')
                    }

                    // FIXME now do associations
                }

                // Hibernate query cache
                'org.hibernate.cache.internal.StandardQueryCache' { bean ->
                    bean.parent = ref('atomicCache')
                }

                // DO THIS IN THE APPLICATION NOT HERE
//                /**
//                 * Delay sessionFactory creation so we can initialize the Ignite grid. The grid must be running when the
//                 * ignite HibernateRegionFactory is initialized
//                 * @see http://burtbeckwith.com/blog/?p=312
//                 * @see http://burtbeckwith.com/blog/?p=1565
//                 */
//                sessionFactory(DelayedSessionFactoryBean) {
//                    def application = AH.application
//                    def ds = application.config.dataSource
//                    def hibConfig = application.config.hibernate
//                    dataSource = ref('dataSource')
//                    List hibConfigLocations = []
//                    if (application.classLoader.getResource('hibernate.cfg.xml')) {
//                        hibConfigLocations << 'classpath:hibernate.cfg.xml'
//                    }
//                    def explicitLocations = hibConfig?.config?.location
//                    if (explicitLocations) {
//                        if (explicitLocations instanceof Collection) {
//                            hibConfigLocations.addAll(explicitLocations.collect { it.toString() })
//                        } else {
//                            hibConfigLocations << hibConfig.config.location.toString()
//                        }
//                    }
//                    configLocations = hibConfigLocations
//                    if (ds.configClass) {
//                        configClass = ds.configClass
//                    }
//                    hibernateProperties = ref('hibernateProperties')
//                    grailsApplication = ref('grailsApplication', true)
//                    lobHandler = ref('lobHandlerDetector')
//                    entityInterceptor = ref('entityInterceptor')
//                    eventListeners = ['flush': new PatchedDefaultFlushEventListener(),
//                            'pre-load': ref('eventTriggeringInterceptor'),
//                            'post-load': ref('eventTriggeringInterceptor'),
//                            'save': ref('eventTriggeringInterceptor'),
//                            'save-update': ref('eventTriggeringInterceptor'),
//                            'post-insert': ref('eventTriggeringInterceptor'),
//                            'pre-update': ref('eventTriggeringInterceptor'),
//                            'post-update': ref('eventTriggeringInterceptor'),
//                            'pre-delete': ref('eventTriggeringInterceptor'),
//                            'post-delete': ref('eventTriggeringInterceptor')]
//                }
//
//                /**
//                 * Delay actual DataSource definition
//                 */
//                dataSource(DelayedDataSource)
            }

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
                        name = IgniteStartupHelper.IGNITE_WEB_SESSION_CACHE_NAME
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

            println "CONFIGURING IGNITE GRID BEAN"

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
        def igniteEnabled = (!(application.config.ignite.enabled instanceof ConfigObject)
                && application.config.ignite.enabled.equals(true))

        if (igniteEnabled) {
            IgniteStartupHelper.startIgnite();
        }
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
