import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.grails.ignite.IgniteStartupHelper

beans {
    def peerClassLoadingEnabledInConfig = (!(application.config.ignite.peerClassLoadingEnabled instanceof ConfigObject)
            && application.config.ignite.peerClassLoadingEnabled.equals(true))



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

    /*
     * Only configure Ignite if the configuration value ignite.enabled=true is defined
     */
    if (igniteEnabled) {
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

            includeEventTypes = [org.apache.ignite.events.EventType.EVT_TASK_STARTED,
                    org.apache.ignite.events.EventType.EVT_TASK_FINISHED,
                    org.apache.ignite.events.EventType.EVT_TASK_FAILED,
                    org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT,
                    org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET,
                    org.apache.ignite.events.EventType.EVT_TASK_REDUCED,
                    org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT,
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
//            bean.dependsOn = ['persistenceInterceptor']
            configuration = ref('igniteCfg')
        }
    }
}