import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.logger.log4j.Log4JLogger
import org.apache.ignite.marshaller.jdk.JdkMarshaller
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.grails.ignite.GroovyOptimizedMarshallerDecorator

class IgniteGrailsPlugin {
    // the plugin version
    def version = "0.1-SNAPSHOT"
    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "2.3 > *"
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
            "grails-app/views/error.gsp"
    ]

    // TODO Fill in these fields
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
    def developers = [ [ name: "Dan Stieglitz", email: "dstieglitz@stainlesscode.com" ]]

    // Location of the plugin's issue tracker.
    def issueManagement = [ system: "GITHUB", url: "https://github.com/dstieglitz/grails-ignite/issues" ]

    // Online location of the plugin's browseable source code.
    def scm = [ url: "https://github.com/dstieglitz/grails-ignite" ]

    def doWithWebDescriptor = { xml ->
        // TODO Implement additions to web.xml (optional), this event occurs before
    }

    def doWithSpring = {
        // TODO Implement runtime spring config (optional)

        // FIXME Caused by ClassNotFoundException: org.apache.ignite.IgniteLogger
        //gridLogger(Log4JLogger)

        igniteCfg(IgniteConfiguration) {
            gridName = "grid"
            peerClassLoadingEnabled = false

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

//            cacheConfiguration = { CacheConfiguration cacheConfiguration ->
//                name = "jobSchedules"
//                cacheMode = CacheMode.REPLICATED
//                atomicityMode = CacheAtomicityMode.ATOMIC
//            }

            includeEventTypes = [org.apache.ignite.events.EventType.EVT_TASK_STARTED,
                    org.apache.ignite.events.EventType.EVT_TASK_FINISHED,
                    org.apache.ignite.events.EventType.EVT_TASK_FAILED,
                    org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT,
                    org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET,
                    org.apache.ignite.events.EventType.EVT_TASK_REDUCED,
                    org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT,
                    org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ,
                    org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ]

//            discoverySpi = { org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi discoverySpi ->
//                ipFinder = { org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder ->
//                    addresses = ['127.0.0.1:47500..47509']
//                }
//            }

            //gridLogger = ref('gridLogger')
        }

        grid(org.apache.ignite.IgniteSpringBean) {
            configuration = ref('igniteCfg')
        }
    }

    def doWithDynamicMethods = { ctx ->
        // TODO Implement registering dynamic methods to classes (optional)
    }

    def doWithApplicationContext = { ctx ->
        // TODO Implement post initialization spring config (optional)
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
