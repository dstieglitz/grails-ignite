package grails.plugins.ignite

import grails.spring.BeanBuilder
import grails.util.Holders
import groovy.transform.Synchronized
import groovy.util.logging.Slf4j
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
@Slf4j
class IgniteStartupHelper {

    static def IGNITE_WEB_SESSION_CACHE_NAME = 'session-cache'
    static def DEFAULT_GRID_NAME = 'grid'
    static String SCHEDULER_SERVICE_NAME = 'distributedSchedulerService'

    private static ApplicationContext igniteApplicationContext
    public static Ignite grid
    private static BeanBuilder igniteBeans = new BeanBuilder()
    private static boolean initialized = false

    public static BeanBuilder getBeans(String resourcePattern, BeanBuilder bb = null) {
        if (bb == null) {
            bb = new BeanBuilder()
        }

        bb.setClassLoader(Thread.currentThread().contextClassLoader)
        Binding binding = new Binding()
        binding.application = Holders.grailsApplication
        bb.setBinding(binding)

        //bb.importBeans(resourcePattern)
        bb.loadBeans(resourcePattern)

        return bb
    }

    @Synchronized
    public static boolean startIgnite() {
        // look for a IgniteResources.groovy file on the classpath
        // load it into an igniteApplicationContext and start ignite
        // merge the application context
        if (initialized) return true

        def igniteEnabled = Holders.grailsApplication.config.getProperty("ignite.enabled", Boolean, false)

        def igniteConfigLocations = Holders.grailsApplication.config.getRequiredProperty("ignite.config.locations", Collection)

        log.debug "startIgnite() --> igniteEnabled=${igniteEnabled}"

        if (igniteEnabled) {
            igniteConfigLocations.each {
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

    @Synchronized
    public static boolean startIgniteFromSpring() {
        if (initialized) return true
        def application = Holders.grailsApplication
        def ctx = igniteApplicationContext

        def configuredGridName = application.config.getProperty("ignite.gridName", String, DEFAULT_GRID_NAME)

        def quiet = application.config.getProperty("ignite.quiet", String, "false")

        // FIXME this doesn't work
        log.info "quiet configured as '$quiet'"
        System.setProperty("IGNITE_QUIET", quiet);

        BeanBuilder cacheBeans = null

        try {
            log.info "looking for cache resources..."
            cacheBeans = getBeans("IgniteCacheResources")
            log.debug "found ${cacheBeans} cache resources"
        } catch (BeanDefinitionParsingException e) {
            log.error e.message
            log.warn "No cache configuration found or cache configuration could not be loaded"
        }

        try {
            grid = ctx.getBean('grid')

            def cacheConfigurationBeans = []

            if (cacheBeans != null) {
                ApplicationContext cacheCtx = cacheBeans.createApplicationContext()
                log.info "found ${cacheCtx.beanDefinitionCount} cache resource beans"
                cacheCtx.beanDefinitionNames.each { beanDefName ->
                    def bean = cacheCtx.getBean(beanDefName)
                    if (bean instanceof CacheConfiguration) {
                        log.info "found manually-configured cache bean ${beanDefName}"
                        cacheConfigurationBeans.add(bean)
                    }
                }
            }

            igniteApplicationContext.beanDefinitionNames.each { beanDefName ->
                def bean = igniteApplicationContext.getBean(beanDefName)
                if (bean instanceof CacheConfiguration) {
                    log.info "found manually-configured cache bean ${beanDefName}"
                    cacheConfigurationBeans.add(bean)
                }
            }

            grid.configuration().setCacheConfiguration(cacheConfigurationBeans.toArray() as CacheConfiguration[])

            log.info "[grails-ignite] Starting Ignite grid..."
            if (grid.respondsTo('start')) grid.start()

            // don't re-deploy the scheduler service
            def schedulerServiceDeployed = false
            grid.services().serviceDescriptors().each { serviceDescriptor ->
                log.info "found deployed service ${serviceDescriptor.name()}"
                if (serviceDescriptor.name().equals(SCHEDULER_SERVICE_NAME)) {
                    schedulerServiceDeployed = true
                }
            }
            if (!schedulerServiceDeployed) {
                def poolSize = Holders.grailsApplication.config.getProperty("ignite.config.executorThreadPoolSize", Long, 10)

                DistributedSchedulerServiceImpl distributedSchedulerServiceImpl = null;

                try {
                    distributedSchedulerServiceImpl = igniteApplicationContext.getBean("distributedSchedulerServiceImpl")
                } catch (NoSuchBeanDefinitionException nsbde) {
                    log.warn "No DistributedSchedulerServiceImpl bean is defined, using default"
                }

                if (distributedSchedulerServiceImpl == null) {
                    distributedSchedulerServiceImpl = new DistributedSchedulerServiceImpl();
                }

                //
                // Allow server startup to continue if running a plugin script, e.g., dbm-generate-changelog
                //
                try {
                    distributedSchedulerServiceImpl.setPoolSize(poolSize)
                    grid.services().deployClusterSingleton(SCHEDULER_SERVICE_NAME, distributedSchedulerServiceImpl);
                    log.info "DistributedSchedulerServiceImpl deployed with bean $distributedSchedulerServiceImpl and poolSize=$poolSize"
                } catch (Throwable t) {
                    log.error "DistributedSchedulerServiceImpl FAILED TO DEPLOY"
                    log.error t.message, t
                }
            }

            initialized = true

        } catch (NoSuchBeanDefinitionException e) {
            log.warn e.message
            return false;
        } catch (IgniteCheckedException e) {
            log.error e.message, e
            return false;
        }
        
        return true;
    }

    public static ApplicationContext getIgniteApplicationContext() {
        return igniteApplicationContext;
    }
}
