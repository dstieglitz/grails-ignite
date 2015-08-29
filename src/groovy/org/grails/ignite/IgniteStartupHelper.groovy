package org.grails.ignite

import grails.spring.BeanBuilder
import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCheckedException
import org.apache.ignite.configuration.CacheConfiguration
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException
import org.springframework.context.ApplicationContext

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 8/23/15
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
@Log4j
class IgniteStartupHelper {

    static def IGNITE_WEB_SESSION_CACHE_NAME = 'session-cache'
    static def DEFAULT_GRID_NAME = 'grid'
    static def IGNITE_CONFIG_DIRECTORY_NAME = 'ignite'

    private static ApplicationContext igniteApplicationContext
    public static Ignite grid
    private static BeanBuilder igniteBeans = new BeanBuilder()

    public static BeanBuilder getBeans(String resourcePattern, BeanBuilder bb = null) {
        if (bb == null) {
            bb = new BeanBuilder()
        }

        bb.setClassLoader(this.class.classLoader)
        Binding binding = new Binding()
        binding.application = Holders.grailsApplication
        bb.setBinding(binding)

//        def pluginDir = GrailsPluginUtils.pluginInfos.find { it.name == 'ignite' }?.pluginDir
//        def defaultUrl = null
//        def url = null
//
//        if (pluginDir != null) {
//            defaultUrl = "file:${pluginDir}/grails-app/conf/spring/${fileName}.groovy"
//            url = "file:grails-app/conf/spring/${fileName}"
//            log.info "loading default configuration from ${defaultUrl}"
//            bb.importBeans(defaultUrl)
//        } else {
//            url = "classpath*:${fileName}*"
//        }
//
//        log.info "attempting to load beans from ${url}"
//        bb.importBeans(url)


        bb.importBeans(resourcePattern)

        return bb
    }

    public static boolean startIgnite() {
        // look for a IgniteResources.groovy file on the classpath
        // load it into an igniteApplicationContext and start ignite
        // merge the application context

        def igniteEnabled = (!(Holders.grailsApplication.config.ignite.enabled instanceof ConfigObject)
                && Holders.grailsApplication.config.ignite.enabled.equals(true))

        if (Holders.grailsApplication.config.ignite.config.locations instanceof ConfigObject) {
            throw new IllegalArgumentException("You must specify the locations to Ignite configuration files in ignite.config.locations, see docs");
        }

        if (!Holders.grailsApplication.config.ignite.config.locations instanceof Collection) {
            throw new IllegalArgumentException("You must specify a collection of resource locations to Ignite configuration files in ignite.config.locations, see docs");
        }

        if (Holders.grailsApplication.config.ignite.config.locations.empty) {
            throw new IllegalArgumentException("You must specify the locations to Ignite configuration files in ignite.config.locations, see docs");
        }

        if (igniteEnabled) {
            Holders.grailsApplication.config.ignite.config.locations.each {
                log.info "loading Ignite beans configuration from ${it}"
                getBeans(it, igniteBeans)
            }

            igniteApplicationContext = igniteBeans.createApplicationContext()

            igniteApplicationContext.beanDefinitionNames.each {
                log.debug "found bean ${it}"
            }

            if (igniteApplicationContext == null) {
                throw new IllegalArgumentException("Unable to initialize");
            }

            return startIgniteFromSpring();
        } else {
            log.warn "startIgnite called, but ignite is not enabled in configuration"
            return false;
        }
    }

    public static boolean startIgniteFromSpring() {
        def application = Holders.grailsApplication
        def ctx = igniteApplicationContext

        def configuredGridName = DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        System.setProperty("IGNITE_QUIET", "false");

        BeanBuilder cacheBeans = null

        try {
            log.info "looking for cache resources..."
            cacheBeans = getBeans("IgniteCacheResources")
        } catch (BeanDefinitionParsingException e) {
            log.error e.message
            log.warn "No cache configuration found or cache configuration could not be loaded"
        }

        try {
            grid = ctx.getBean('grid')

            def cacheConfigurationBeans = []
//            if (cacheBeans != null) {
//                ApplicationContext cacheCtx = cacheBeans.createApplicationContext()
//                log.info "found ${cacheCtx.beanDefinitionCount} cache resource beans"
                igniteApplicationContext.beanDefinitionNames.each { beanDefName ->
                    def bean = igniteApplicationContext.getBean(beanDefName)
                    if (bean instanceof CacheConfiguration) {
                        log.info "found manually-configured cache bean ${beanDefName}"
                        cacheConfigurationBeans.add(bean)
                    }
                }

                grid.configuration().setCacheConfiguration(cacheConfigurationBeans.toArray() as CacheConfiguration[])
//            }

//            println grid.configuration().cacheConfiguration
            // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
//            grid.configuration().setGridLogger(new Log4JLogger())

            //           if (grid.state() != IgniteState.STARTED) {
            log.info "Starting Ignite grid..."
            grid.start()
            grid.services().deployClusterSingleton("distributedSchedulerService", new DistributedSchedulerServiceImpl());
//            }

        } catch (NoSuchBeanDefinitionException e) {
            log.warn e.message
            return false;
        } catch (IgniteCheckedException e) {
            log.error e.message, e
            return false;
        }

//        ctx.getBean('distributedSchedulerService').grid = grid
        return true;
    }

//    public static CacheConfiguration getSpringConfiguredCache(String name) {
//        try {
//            return igniteApplicationContext.getBean(name)
//        } catch (NoSuchBeanDefinitionException e) {
//            return null;
//        }
//    }
//
//    public static boolean startIgniteProgramatically() {
//        def ctx = Holders.applicationContext
//        def application = Holders.grailsApplication
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
//
//        return grid != null
//    }
//
//    public static ApplicationContext getApplicationContext() {
//        return igniteApplicationContext;
//    }
}
