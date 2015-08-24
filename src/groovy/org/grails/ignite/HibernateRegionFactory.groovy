package org.grails.ignite

import grails.util.Holders
import org.apache.log4j.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.*;
import org.hibernate.cache.spi.access.AccessType;
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
        log.debug("Ignite HibernateRegionFactory start()");

        IgniteStartupHelper.applicationContext.beanDefinitionNames.each {
            println it
        }

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
        return underlyingRegionFactory.buildEntityRegion(s, properties, cacheDataDescription);
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String s, Properties properties, CacheDataDescription cacheDataDescription) throws CacheException {
        return underlyingRegionFactory.buildNaturalIdRegion(s, properties, cacheDataDescription);
    }

    @Override
    public CollectionRegion buildCollectionRegion(String s, Properties properties, CacheDataDescription cacheDataDescription) throws CacheException {
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
}
