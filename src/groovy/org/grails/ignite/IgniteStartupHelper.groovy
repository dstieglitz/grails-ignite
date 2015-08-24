package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.IgniteCheckedException
import org.apache.ignite.IgniteState
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.springframework.beans.BeansException
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 8/23/15
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
@Log4j
class IgniteStartupHelper implements ApplicationContextAware {

    private static ApplicationContext applicationContext

    static def IGNITE_WEB_SESSION_CACHE_NAME = 'session-cache'
    static def DEFAULT_GRID_NAME = 'grid'

    public static boolean startIgnite() {
        return startIgniteFromSpring();
    }

    public static boolean startIgniteFromSpring() {
//        def ctx = Holders.applicationContext
        def ctx = applicationContext
        def application = Holders.grailsApplication

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
            // FIXME https://github.com/dstieglitz/grails-ignite/issues/1
//            grid.configuration().setGridLogger(new Log4JLogger())

            if (grid.state() != IgniteState.STARTED) {
                log.info "Starting Ignite grid..."
                grid.start()
                grid.services().deployClusterSingleton("distributedSchedulerService", new DistributedSchedulerServiceImpl());
            }

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

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
