import grails.plugins.ignite.DeferredStartIgniteSpringBean
import org.apache.ignite.configuration.IgniteConfiguration
import grails.plugins.ignite.DistributedSchedulerServiceImpl
import grails.plugins.ignite.IgniteGrailsLogger
import grails.plugins.ignite.IgniteStartupHelper

//import org.apache.ignite.logger.log4j.Log4JLogger
beans {

    def peerClassLoadingEnabledInConfig = application.config.getProperty("ignite.peerClassLoadingEnabled", Boolean, false)

    def configuredGridName = application.config.getProperty("ignite.gridName", String, IgniteStartupHelper.DEFAULT_GRID_NAME)

    def configuredNetworkTimeout = application.config.getProperty("ignite.discoverySpi.networkTimeout", Long, 3000)

    def configuredAckTimeout = application.config.getProperty("ignite.discoverySpi.ackTimeout", Long, 10000)

    def configuredAddresses = application.config.getProperty("ignite.discoverySpi.addresses", Collection, [])

    def igniteEnabled = application.config.getProperty("ignite.enabled", Boolean, false)

    def s3DiscoveryEnabled = application.config.getProperty("ignite.discoverySpi.s3Discovery", Boolean, false)

    def multicastDiscoveryEnabled = application.config.getProperty("ignite.discoverySpi.multicastDiscovery", Boolean, false)

    /*
     * Only configure Ignite if the configuration value ignite.enabled=true is defined
     */
    if (igniteEnabled) {
        // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
        gridLogger(IgniteGrailsLogger)

        igniteCfg(IgniteConfiguration) {
            igniteInstanceName = configuredGridName
            peerClassLoadingEnabled = peerClassLoadingEnabledInConfig

            includeEventTypes = [org.apache.ignite.events.EventType.EVT_TASK_STARTED,
                                 org.apache.ignite.events.EventType.EVT_TASK_FINISHED,
                                 org.apache.ignite.events.EventType.EVT_TASK_FAILED,
                                 org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT,
                                 org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET,
                                 org.apache.ignite.events.EventType.EVT_TASK_REDUCED,
                                 org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT,
                                 org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ]

            if (s3DiscoveryEnabled) {
                def accessKey = application.config.getRequiredProperty("ignite.discoverySpi.awsAccessKey")
                def secretKey = application.config.getRequiredProperty("ignite.discoverySpi.awsSecretKey")
                def theBucketName = application.config.getRequiredProperty("ignite.discoverySpi.s3DiscoveryBucketName")
                discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
                    ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder tcpDiscoveryS3IpFinder ->
                        bucketName = theBucketName
                        awsCredentials = ref('awsCredentials')
                    }
                }
                awsCredentials(com.amazonaws.auth.BasicAWSCredentials) { bean ->
                    bean.constructorArgs = [accessKey, secretKey]
                }
                awsCredentialsProvider(com.amazonaws.auth.AWSStaticCredentialsProvider) { bean ->
                    bean.constructorArgs = [ref('awsCredentials')]
                }
            } else if (multicastDiscoveryEnabled) {
                discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
                    networkTimeout = configuredNetworkTimeout
                    ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder ->
                        addresses = configuredAddresses
                    }
                }
            } else {
                discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
                    networkTimeout = configuredNetworkTimeout
                    ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder ->
                        addresses = configuredAddresses
                    }
                }
            }

            gridLogger = ref('gridLogger')
        }

        distributedSchedulerServiceImpl(DistributedSchedulerServiceImpl)

        grid(DeferredStartIgniteSpringBean) { bean ->
            bean.lazyInit = true
//            bean.dependsOn = ['persistenceInterceptor']
            configuration = ref('igniteCfg')
        }
    }
}