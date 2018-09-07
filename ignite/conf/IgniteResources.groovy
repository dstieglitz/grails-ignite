import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.grails.ignite.DistributedSchedulerServiceImpl
import org.grails.ignite.IgniteGrailsLogger
import org.grails.ignite.IgniteStartupHelper

//import org.apache.ignite.logger.log4j.Log4JLogger
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

    def configuredAckTimeout = 10000
    if (!(application.config.ignite.discoverySpi.ackTimeout instanceof ConfigObject)) {
        configuredAckTimeout = application.config.ignite.discoverySpi.ackTimeout
    }

    def configuredAddresses = []
    if (!(application.config.ignite.discoverySpi.addresses instanceof ConfigObject)) {
        configuredAddresses = application.config.ignite.discoverySpi.addresses
    }

    def igniteEnabled = (!(application.config.ignite.enabled instanceof ConfigObject)
            && application.config.ignite.enabled.equals(true))

    def s3DiscoveryEnabled = (!(application.config.ignite.discoverySpi.s3Discovery instanceof ConfigObject)
            && application.config.ignite.discoverySpi.s3Discovery.equals(true))

    def multicastDiscoveryEnabled = (!(application.config.ignite.discoverySpi.multicastDiscovery instanceof ConfigObject)
            && application.config.ignite.discoverySpi.multicastDiscovery.equals(true))

    /*
     * Only configure Ignite if the configuration value ignite.enabled=true is defined
     */
    if (igniteEnabled) {
        // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
        gridLogger(IgniteGrailsLogger)

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

            if (s3DiscoveryEnabled) {
                def accessKey = application.config.ignite.discoverySpi.awsAccessKey
                def secretKey = application.config.ignite.discoverySpi.awsSecretKey
                def theBucketName = application.config.ignite.discoverySpi.s3DiscoveryBucketName
                if (accessKey instanceof ConfigObject) {
                    throw new IllegalArgumentException("You must provide an AWS access key for s3-based discovery");
                }
                if (secretKey instanceof ConfigObject) {
                    throw new IllegalArgumentException("You must provide an AWS secret key for s3-based discovery");
                }
                if (theBucketName instanceof ConfigObject) {
                    throw new IllegalArgumentException("You must provide an AWS S3 bucket name for s3-based discovery");
                }
                discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
                    ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder tcpDiscoveryS3IpFinder ->
                        bucketName = theBucketName
                        awsCredentials = ref('awsCredentials')
                    }
                }
                awsCredentials(com.amazonaws.auth.BasicAWSCredentials) { bean ->
                    bean.constructorArgs = [accessKey, secretKey]
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

        grid(org.grails.ignite.DeferredStartIgniteSpringBean) { bean ->
            bean.lazyInit = true
//            bean.dependsOn = ['persistenceInterceptor']
            configuration = ref('igniteCfg')
        }
    }
}