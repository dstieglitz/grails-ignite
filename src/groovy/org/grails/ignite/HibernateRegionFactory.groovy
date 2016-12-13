package org.grails.ignite

import grails.util.Holders
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.log4j.Logger
import org.hibernate.cache.CacheException
import org.hibernate.cache.spi.*
import org.hibernate.cache.spi.access.AccessType
import org.hibernate.cfg.Settings

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 8/23/15
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class HibernateRegionFactory implements RegionFactory {

    private static final Logger log = Logger.getLogger(HibernateRegionFactory.class.getName());
    private org.apache.ignite.cache.hibernate.HibernateRegionFactory underlyingRegionFactory;
    private boolean igniteNodeInitialized;

    public HibernateRegionFactory() {
        if (Holders.config.ignite.l2CacheEnabled == false) {
            throw new RuntimeException("Can't initialize HibernateRegionFactory, l2Cache is disable in config.");
        }
        underlyingRegionFactory = new org.apache.ignite.cache.hibernate.HibernateRegionFactory();
    }

    private boolean init() {
        log.debug "init() --> igniteNodeInitialized=${igniteNodeInitialized}"
        if (igniteNodeInitialized) {
            return;
        }

        if (IgniteStartupHelper.startIgnite()) {
            igniteNodeInitialized = true;
            return true;
        }

        return false;
    }

    @Override
    public void start(Settings settings, Properties properties) throws CacheException {
        log.debug("Ignite HibernateRegionFactory start() with settings=${settings}, properties=${properties}");

        //
        // we need to re-write property names here, Grails will prepend "hibernate." to them
        //
        def gridName = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_NAME_PROPERTY}")
        def dfltCacheNameProperty = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_CACHE_NAME_PROPERTY}")
        def regionCacheProperty = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.REGION_CACHE_PROPERTY}")
        def defaultAccessTypeProperty = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_ACCESS_TYPE_PROPERTY}")
        def gridConfigProperty = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_CONFIG_PROPERTY}")

        log.info "grid name is ${gridName}"
        if (gridName) {
            properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_NAME_PROPERTY, gridName)
        }
        log.info "default cache name is ${dfltCacheNameProperty}"
        if (dfltCacheNameProperty) {
            properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_CACHE_NAME_PROPERTY, dfltCacheNameProperty)
        }
        log.info "region cache is ${regionCacheProperty}"
        if (regionCacheProperty) {
            properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.REGION_CACHE_PROPERTY, regionCacheProperty)
        }
        log.info "defaultAccessTypeProperty is ${defaultAccessTypeProperty}"
        if (defaultAccessTypeProperty) {
            properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_ACCESS_TYPE_PROPERTY, defaultAccessTypeProperty)
        }
        log.info "grid config is ${gridConfigProperty}"
        if (gridConfigProperty) {
            properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_CONFIG_PROPERTY, gridConfigProperty)
        }

        if (init()) {
            log.debug "starting underlyingRegionFactory"
            underlyingRegionFactory.start(settings, properties);
        }
    }

    @Override
    public void stop() {
        log.debug("Ignite HibernateRegionFactory stop()");
        underlyingRegionFactory.stop();
    }

    @Override
    public boolean isMinimalPutsEnabledByDefault() {
        return underlyingRegionFactory.isMinimalPutsEnabledByDefault();
    }

    @Override
    public AccessType getDefaultAccessType() {
        return underlyingRegionFactory.getDefaultAccessType();
    }

    @Override
    public long nextTimestamp() {
        return underlyingRegionFactory.nextTimestamp();
    }

    @Override
    public EntityRegion buildEntityRegion(String s, Properties properties, CacheDataDescription cacheDataDescription) throws CacheException {
        configureEntityCache(s);
        return underlyingRegionFactory.buildEntityRegion(s, properties, cacheDataDescription);
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String s, Properties properties, CacheDataDescription cacheDataDescription) throws CacheException {
        return underlyingRegionFactory.buildNaturalIdRegion(s, properties, cacheDataDescription);
    }

    @Override
    public CollectionRegion buildCollectionRegion(String s, Properties properties, CacheDataDescription cacheDataDescription) throws CacheException {
        // check if the cache exists, create it if not
        log.trace "buildCollectionRegion(${s}, ${properties}, ${cacheDataDescription})"
        configureAssociationCache(s);

        return underlyingRegionFactory.buildCollectionRegion(s, properties, cacheDataDescription);
    }

    @Override
    public QueryResultsRegion buildQueryResultsRegion(String s, Properties properties) throws CacheException {
        return underlyingRegionFactory.buildQueryResultsRegion(s, properties);
    }

    @Override
    public TimestampsRegion buildTimestampsRegion(String s, Properties properties) throws CacheException {
        return underlyingRegionFactory.buildTimestampsRegion(s, properties);
    }

    private void configureEntityCache(String entityName) {
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll {
            it.name.equals(entityName)
        }.size()

        if (configuredCaches == 0) {
//            def grailsDomainClass = Holders.grailsApplication.getDomainClass(entityName);
//            log.debug "interrogating grails domain class ${entityName} for cache information"
//            log.debug "creating default cache for ${entityName}"

            CacheConfiguration cc = IgniteCacheConfigurationFactory.getCacheConfiguration("ignite.l2Cache.entity", entityName)

            IgniteStartupHelper.grid.getOrCreateCache(cc);
        }
    }

    private void configureAssociationCache(String associationName) {
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll {
            it.name.equals(associationName)
        }.size()

        if (configuredCaches == 0) {
            def grailsDomainClassName = associationName.substring(0, associationName.lastIndexOf('.'));
            log.debug "interrogating grails domain class ${grailsDomainClassName} for cache information"
            log.debug "creating default cache for ${associationName}"

            CacheConfiguration cc = IgniteCacheConfigurationFactory.getCacheConfiguration("ignite.l2Cache.association", associationName)

            IgniteStartupHelper.grid.getOrCreateCache(cc);
        }
    }

}
