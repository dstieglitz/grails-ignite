package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMemoryMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy
import org.apache.ignite.configuration.CacheConfiguration

/**
 * Created by dstieglitz on 2/21/16.
 */
@Log4j
class IgniteCacheConfigurationFactory {

    static CacheConfiguration getCacheConfiguration(String name) {
        def cacheMode = getConfigValueOrThrowException("ignite.defaultCache.cacheMode", Holders.config.ignite.defaultCache.cacheMode)
        def memoryMode = getConfigValueOrThrowException("ignite.defaultCache.memoryMode", Holders.config.ignite.defaultCache.memoryMode)
        def atomicityMode = getConfigValueOrThrowException("ignite.defaultCache.atomicityMode", Holders.config.ignite.defaultCache.atomicityMode)
        def writeSynchronizationMode = getConfigValueOrThrowException("ignite.defaultCache.writeSynchronizationMode", Holders.config.ignite.defaultCache.writeSynchronizationMode)
        def offHeapMaxMemory = getConfigValueOrThrowException("ignite.defaultCache.offHeapMaxMemory", Holders.config.ignite.defaultCache.offHeapMaxMemory)
        def maxElements = getConfigValueOrThrowException("ignite.defaultCache.maxElements", Holders.config.ignite.defaultCache.maxElements)
        def swapEnabled = getConfigValueOrThrowException("ignite.defaultCache.swapEnabled", Holders.config.ignite.defaultCache.swapEnabled)
        def evictionPolicy = getConfigValueOrThrowException("ignite.defaultCache.evictionPolicy", Holders.config.ignite.defaultCache.evictionPolicy)
        def statisticsEnabled = getConfigValueOrThrowException("ignite.defaultCache.statisticsEnabled", Holders.config.ignite.defaultCache.statisticsEnabled)
        def managementEnabled = getConfigValueOrThrowException("ignite.defaultCache.managementEnabled", Holders.config.ignite.defaultCache.managementEnabled)

        if (evictionPolicy != 'lru') {
            throw new IllegalArgumentException("Eviction policy ${evictionPolicy} not supported")
        }

        return getCacheConfigurationLru(name,
                cacheMode,
                memoryMode,
                atomicityMode,
                writeSynchronizationMode,
                (long) offHeapMaxMemory,
                maxElements,
                swapEnabled,
                statisticsEnabled,
                managementEnabled)

        // TODO get this from configuration
        //return getDefaultCacheConfiguration(name)
    }

    /**
     * Sensible defaults
     * @param name
     * @return
     */
    static CacheConfiguration getDefaultCacheConfiguration(String name) {
        def config = new CacheConfiguration(name);
        config.setCacheMode(CacheMode.PARTITIONED)
        config.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC)
        config.setOffHeapMaxMemory(1L * 1024L * 1024L * 1024L); // 1 GIG
        config.setEvictionPolicy(new LruEvictionPolicy(10000));
        config.setSwapEnabled(true)

        return config
    }

    /**
     * Sensible defaults
     * @param name
     * @return
     */
    static CacheConfiguration getCacheConfigurationLru(String name,
                                                       CacheMode cacheMode,
                                                       CacheMemoryMode cacheMemoryMode,
                                                       CacheAtomicityMode cacheAtomicityMode,
                                                       CacheWriteSynchronizationMode synchronizationMode,
                                                       long offHeapMaxMemory,
                                                       int maxElements,
                                                       boolean swapEnabled,
                                                       boolean statisticsEnabled,
                                                       boolean managementEnabled) {
        def config = new CacheConfiguration(name);
        config.setCacheMode(cacheMode)
        config.setMemoryMode(cacheMemoryMode)
        config.setWriteSynchronizationMode(synchronizationMode)
        config.setAtomicityMode(cacheAtomicityMode)
        config.setOffHeapMaxMemory(offHeapMaxMemory);
        config.setEvictionPolicy(new LruEvictionPolicy(maxElements));
        config.setSwapEnabled(swapEnabled)
        config.setStatisticsEnabled(statisticsEnabled)
        config.setManagementEnabled(managementEnabled)

        return config
    }

    private static def getConfigValueOrThrowException(name, val) {
        log.debug "getConfigValueOrThrowException ${val}"

        if (val instanceof ConfigObject) {
            throw new IllegalArgumentException("Config parameter ${name} must be defined")
        } else {
            return val
        }
    }
}
