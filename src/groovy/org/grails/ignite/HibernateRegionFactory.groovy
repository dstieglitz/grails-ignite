package org.grails.ignite

import grails.util.Holders
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.log4j.Logger
import org.codehaus.groovy.grails.orm.hibernate.cfg.GrailsDomainBinder
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
public class HibernateRegionFactory implements org.hibernate.cache.spi.RegionFactory {

    private static final Logger log = Logger.getLogger(HibernateRegionFactory.class.getName());
    private org.apache.ignite.cache.hibernate.HibernateRegionFactory underlyingRegionFactory;
    private boolean igniteNodeInitialized;

    public HibernateRegionFactory() {
        underlyingRegionFactory = new org.apache.ignite.cache.hibernate.HibernateRegionFactory();
    }

    private boolean init() {
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

        // we need to re-write property names here, Grails will prepend "hibernate." to them
        def gridName = properties.getProperty("hibernate.${org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_NAME_PROPERTY}")
        if (gridName) properties.setProperty(org.apache.ignite.cache.hibernate.HibernateRegionFactory.GRID_NAME_PROPERTY, gridName)

        if (init()) {
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
        log.debug "buildCollectionRegion(${s}, ${properties}, ${cacheDataDescription})"
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
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll { it.name.equals(entityName) }.size()

        if (configuredCaches == 0) {
//            def springConfiguration = IgniteStartupHelper.getSpringConfiguredCache(entityName)
//            if (springConfiguration != null) {
//                log.info "found a manually-configured cache for ${entityName}, will configure from external configuration"
//                IgniteStartupHelper.grid.addCacheConfiguration(springConfiguration);
//            } else {
                def grailsDomainClass = Holders.grailsApplication.getDomainClass(entityName);
                log.debug "interrogating grails domain class ${entityName} for cache information"
                log.debug "creating default cache for ${entityName}"
                CacheConfiguration cc = new CacheConfiguration(entityName);
                def binder = new GrailsDomainBinder()
                def mapping = binder.getMapping(grailsDomainClass);
                log.debug "found mapping ${mapping} for ${grailsDomainClass}"
                cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
//            if (mapping?.cache?.usage?.equalsIgnoreCase("read-write")) {
//
//            }

                IgniteStartupHelper.grid.getOrCreateCache(cc);
//            }
        }
    }

    private void configureAssociationCache(String associationName) {
        def configuredCaches = IgniteStartupHelper.grid.configuration().getCacheConfiguration().findAll { it.name.equals(associationName) }.size()

        if (configuredCaches == 0) {
//            def springConfiguration = IgniteStartupHelper.getSpringConfiguredCache(associationName);
//            if (springConfiguration != null) {
//                log.info "found a manually-configured cache for ${associationName}, will configure from external configuration"
//                IgniteStartupHelper.grid.addCacheConfiguration(springConfiguration);
//            } else {
                def grailsDomainClassName = associationName.substring(0, associationName.lastIndexOf('.'));
                def grailsDomainClass = Holders.grailsApplication.getDomainClass(grailsDomainClassName);
                log.debug "interrogating grails domain class ${grailsDomainClassName} for cache information"
                log.debug "creating default cache for ${associationName}"
                CacheConfiguration cc = new CacheConfiguration(associationName);
                def binder = new GrailsDomainBinder()
                def mapping = binder.getMapping(grailsDomainClass);
                log.debug "found mapping ${mapping} for ${grailsDomainClass}"
                cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
//            if (mapping?.cache?.usage?.equalsIgnoreCase("read-write")) {
//
//            }

                IgniteStartupHelper.grid.getOrCreateCache(cc);
//            }
        }
    }
}