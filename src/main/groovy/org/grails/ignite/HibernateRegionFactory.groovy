package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Slf4j
import org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory
import org.apache.ignite.configuration.CacheConfiguration
import org.hibernate.boot.spi.SessionFactoryOptions
import org.hibernate.cache.CacheException
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig
import org.hibernate.cache.spi.*
import org.hibernate.cache.spi.access.AccessType
import org.hibernate.engine.spi.SessionFactoryImplementor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 8/23/15
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
@Slf4j
class HibernateRegionFactory implements RegionFactory {

    private org.apache.ignite.cache.hibernate.HibernateRegionFactory underlyingRegionFactory
    private boolean igniteNodeInitialized

    HibernateRegionFactory() {
        if (Holders.config.getProperty('ignite.l2CacheEnabled', Boolean, false)) {
            throw new RuntimeException("Can't initialize HibernateRegionFactory, l2Cache is disabled in config.")
        }
        underlyingRegionFactory = new org.apache.ignite.cache.hibernate.HibernateRegionFactory()
    }

    private boolean init() {
        log.debug "init() --> igniteNodeInitialized=${igniteNodeInitialized}"
        if (igniteNodeInitialized) {
            return
        }

        if (IgniteStartupHelper.startIgnite()) {
            igniteNodeInitialized = true
            return true
        }

        return false
    }

    @Override
    void stop() {
        log.debug("Ignite HibernateRegionFactory stop()")
        underlyingRegionFactory.stop()
    }

    @Override
    void start(SessionFactoryOptions settings, Map configValues) throws CacheException {
//        log.debug("Ignite HibernateRegionFactory start() with settings=${settings}, configValues=${configValues}")

        //
        // we need to re-write property names here, Grails will prepend "hibernate." to them
        //
        def gridName = configValues["hibernate.${HibernateAccessStrategyFactory.IGNITE_INSTANCE_NAME_PROPERTY}"]
        def regionCacheProperty = configValues["hibernate.${HibernateAccessStrategyFactory.REGION_CACHE_PROPERTY}"]
        def defaultAccessTypeProperty = configValues["hibernate.${HibernateAccessStrategyFactory.DFLT_ACCESS_TYPE_PROPERTY}"]
        def gridConfigProperty = configValues["hibernate.${HibernateAccessStrategyFactory.GRID_CONFIG_PROPERTY}"]

        log.info "grid name is ${gridName}"
        if (gridName) {
            configValues[HibernateAccessStrategyFactory.IGNITE_INSTANCE_NAME_PROPERTY] = gridName
        }
        log.info "region cache is ${regionCacheProperty}"
        if (regionCacheProperty) {
            configValues[HibernateAccessStrategyFactory.REGION_CACHE_PROPERTY] = regionCacheProperty
        }
        log.info "defaultAccessTypeProperty is ${defaultAccessTypeProperty}"
        if (defaultAccessTypeProperty) {
            configValues[HibernateAccessStrategyFactory.DFLT_ACCESS_TYPE_PROPERTY] = defaultAccessTypeProperty
        }
        log.info "grid config is ${gridConfigProperty}"
        if (gridConfigProperty) {
            configValues[HibernateAccessStrategyFactory.GRID_CONFIG_PROPERTY] = gridConfigProperty
        }

        if (init()) {
            log.debug "starting underlyingRegionFactory"
            underlyingRegionFactory.start(settings, configValues)
        }

    }

    @Override
    boolean isMinimalPutsEnabledByDefault() {
        return underlyingRegionFactory.isMinimalPutsEnabledByDefault()
    }

    @Override
    AccessType getDefaultAccessType() {
        return underlyingRegionFactory.getDefaultAccessType()
    }

    @Override
    String qualify(String regionName) {
        return null
    }

    @Override
    long nextTimestamp() {
        return underlyingRegionFactory.nextTimestamp()
    }

    @Override
    DomainDataRegion buildDomainDataRegion(DomainDataRegionConfig regionConfig, DomainDataRegionBuildingContext buildingContext) {
        configureEntityCache(regionConfig.regionName)
        regionConfig.collectionCaching.each {
            //FIXME: How can we create association caches? There is no name to retrieve
        }
        return underlyingRegionFactory.buildDomainDataRegion(regionConfig, buildingContext)
    }

    @Override
    QueryResultsRegion buildQueryResultsRegion(String regionName, SessionFactoryImplementor sessionFactory) {
        return underlyingRegionFactory.buildQueryResultsRegion(regionName, sessionFactory)
    }

    @Override
    TimestampsRegion buildTimestampsRegion(String regionName, SessionFactoryImplementor sessionFactory) {
        return underlyingRegionFactory.buildTimestampsRegion(regionName, sessionFactory)
    }

    private void configureEntityCache(String entityName) {
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll {
            it.name.equals(entityName)
        }.size()

        if (configuredCaches == 0) {
//            def grailsDomainClass = Holders.grailsApplication.getDomainClass(entityName)
//            log.debug "interrogating grails domain class ${entityName} for cache information"
//            log.debug "creating default cache for ${entityName}"

            CacheConfiguration cc = IgniteCacheConfigurationFactory.getCacheConfiguration("ignite.l2Cache.entity", entityName)

            IgniteStartupHelper.grid.getOrCreateCache(cc)
        }
    }

    private void configureAssociationCache(String associationName) {
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll {
            it.name.equals(associationName)
        }.size()

        if (configuredCaches == 0) {
            def grailsDomainClassName = associationName.substring(0, associationName.lastIndexOf('.'))
            log.debug "interrogating grails domain class ${grailsDomainClassName} for cache information"
            log.debug "creating default cache for ${associationName}"

            CacheConfiguration cc = IgniteCacheConfigurationFactory.getCacheConfiguration("ignite.l2Cache.association", associationName)

            IgniteStartupHelper.grid.getOrCreateCache(cc)
        }
    }

}
