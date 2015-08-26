package org.grails.ignite

import grails.spring.BeanBuilder
import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.IgniteCheckedException
import org.apache.ignite.IgniteState
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.codehaus.groovy.grails.plugins.GrailsPluginUtils
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

    private static ApplicationContext igniteApplicationContext

    static def IGNITE_WEB_SESSION_CACHE_NAME = 'session-cache'
    static def DEFAULT_GRID_NAME = 'grid'

    public static boolean startIgnite() {
        // look for a IgniteResources.groovy file on the classpath
        // load it into an igniteApplicationContext and start ignite
        // merge the application context
        def bb = new BeanBuilder(Holders.applicationContext)
        bb.setClassLoader(this.class.classLoader)
        def binding = new Binding()
        binding.application = Holders.grailsApplication
        bb.setBinding(binding)

        // FIXME instead, load defaults and overwrite with what's in application
        try {
            bb.importBeans("file:grails-app/conf/spring/IgniteResources.groovy")
        } catch (BeanDefinitionParsingException e) {
            log.info "loading default Ignite configuration"
            def pluginDir = GrailsPluginUtils.pluginInfos.find { it.name == 'ignite' }.pluginDir
            bb.importBeans("file:${pluginDir}/grails-app/conf/spring/IgniteResources.groovy")
        }

        igniteApplicationContext = bb.createApplicationContext()

        igniteApplicationContext.beanDefinitionNames.each {
            log.info "found bean ${it}"
        }

        if (igniteApplicationContext == null) {
            throw new IllegalArgumentException("Unable to initialize");
        }

        return startIgniteFromSpring();
    }

    public static boolean startIgniteFromSpring() {
        def application = Holders.grailsApplication
        def ctx = igniteApplicationContext

        def configuredGridName = DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        System.setProperty("IGNITE_QUIET", "false");

        //
        // FIXME need to start grid LAST, can't configure with spring this way. Maybe wrap in a delayed start
        // bean or something
        //

        try {
            def grid = ctx.getBean('grid')
            println grid.configuration().cacheConfiguration
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

    public static boolean startIgniteProgramatically() {
        def ctx = Holders.applicationContext
        def application = Holders.grailsApplication

        def configuredAddresses = []
        if (!(application.config.ignite.discoverySpi.addresses instanceof ConfigObject)) {
            configuredAddresses = application.config.ignite.discoverySpi.addresses
        }

        IgniteConfiguration config = new IgniteConfiguration();
        config.setMarshaller(new OptimizedMarshaller(false));
        def discoverySpi = new org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi();
        discoverySpi.setNetworkTimeout(5000);
        def ipFinder = new org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(configuredAddresses)
        discoverySpi.setIpFinder(ipFinder)
        def grid = Ignition.start(config);

        return grid != null
    }

    public static ApplicationContext getApplicationContext() {
        return igniteApplicationContext;
    }
}
