package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Slf4j
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.apache.ignite.configuration.CacheConfiguration

/**
 * Created by dstieglitz on 2/21/16.
 *
 * Adjusted for Ignite 2.0 where many properties have been removed/changed.
 * @see https://cwiki.apache.org/confluence/display/IGNITE/Apache+Ignite+2.0+Migration+Guide#ApacheIgnite2.0MigrationGuide-IgniteConfiguration
 *
 *
 */
@Slf4j
class IgniteCacheConfigurationFactory {

    static CacheConfiguration getCacheConfiguration(String name) {
        return getCacheConfiguration("ignite.defaultCache", name);
    }

    static CacheConfiguration getCacheConfiguration(String prefix, String name) {
        log.debug "getCacheConfiguration ${prefix}, ${name}"

        def cacheModeStr = Holders.config.getProperty("${prefix}.cacheMode", String)
        CacheMode cacheMode = cacheModeStr ? CacheMode.valueOf(cacheModeStr) : getDefaultCacheConfiguration(name).cacheMode
        log.debug "cacheMode=${cacheMode}"

//        def memoryMode = getConfigValueOrNull("${prefix}.memoryMode", flatConfig.get("${prefix}.memoryMode".toString()))
//        if (memoryMode == null) {
//            memoryMode = getDefaultCacheConfiguration(name).memoryMode
//        }
//        log.debug "memoryMode=${memoryMode}"

        def atomicityModeStr = Holders.config.getProperty("${prefix}.atomicityMode", String)
        CacheAtomicityMode atomicityMode = atomicityModeStr ? CacheAtomicityMode.valueOf(atomicityModeStr) : getDefaultCacheConfiguration(name).atomicityMode
        log.debug "atomicityMode=${atomicityMode}"

        def writeSynchronizationModeStr = Holders.config.getProperty("${prefix}.writeSynchronizationMode", String)
        CacheWriteSynchronizationMode writeSynchronizationMode = writeSynchronizationModeStr ? CacheWriteSynchronizationMode.valueOf(writeSynchronizationModeStr) : getDefaultCacheConfiguration(name).writeSynchronizationMode
        log.debug "writeSynchronizationMode=${writeSynchronizationMode}"

//        def offHeapMaxMemory = getConfigValueOrZero("${prefix}.offHeapMaxMemory", flatConfig.get("${prefix}.offHeapMaxMemory".toString()))
//        if (offHeapMaxMemory == null) {
//            offHeapMaxMemory = getDefaultCacheConfiguration(name).offHeapMaxMemory
//        }
//        log.debug "offHeapMaxMemory=${offHeapMaxMemory}"

        def maxElements = Holders.config.getProperty("${prefix}.maxElements", Integer, Holders.config.getProperty("ignite.defaultCache.maxElements", Integer, 0))
        log.debug "maxElements=${maxElements}"

//        def swapEnabled = getConfigValueOrNull("${prefix}.swapEnabled", flatConfig.get("${prefix}.swapEnabled".toString()))
//        if (swapEnabled == null) {
//            swapEnabled = getDefaultCacheConfiguration(name).swapEnabled
//        }
//        log.debug "swapEnabled=${swapEnabled}"

        def evictionPolicy = Holders.config.getProperty("${prefix}.evictionPolicy")
//        if (evictionPolicy == null) {
//            evictionPolicy = 'lru'
//        }
        log.debug "evictionPolicy=${evictionPolicy}"

        def statisticsEnabled = Holders.config.getProperty("${prefix}.statisticsEnabled", Boolean, getDefaultCacheConfiguration(name).statisticsEnabled)
        log.debug "statisticsEnabled=${statisticsEnabled}"

        def managementEnabled = Holders.config.getProperty("${prefix}.managementEnabled", Boolean, getDefaultCacheConfiguration(name).managementEnabled)
        log.debug "managementEnabled=${managementEnabled}"

        def backups = Holders.config.getProperty("${prefix}.backups", Integer, getDefaultCacheConfiguration(name).backups)
        log.debug "backups=${backups}"

        def copyOnReadEnabled = Holders.config.getProperty("${prefix}.copyOnRead", Boolean, getDefaultCacheConfiguration(name).copyOnRead)
        log.debug "copyOnReadEnabled=${copyOnReadEnabled}"

//        def evictSynchronizedEnabled = getConfigValueOrNull("${prefix}.evictSynchronized", flatConfig.get("${prefix}.evictSynchronized".toString()))
//        if (evictSynchronizedEnabled == null) {
//            evictSynchronizedEnabled = getDefaultCacheConfiguration(name).evictSynchronized
//        }
//        log.debug "evictSynchronizedEnabled=${evictSynchronizedEnabled}"

        if (evictionPolicy && evictionPolicy != 'lru') {
            throw new IllegalArgumentException("Eviction policy ${evictionPolicy} not supported")
        }

        return getCacheConfigurationLru(name,
                cacheMode,
//                memoryMode,
                atomicityMode,
                writeSynchronizationMode,
//                (long) offHeapMaxMemory,
                maxElements,
                backups,
//                swapEnabled,
                statisticsEnabled,
                managementEnabled,
                copyOnReadEnabled)
//                evictSynchronizedEnabled)

        // TODO get this from configuration
        //return getDefaultCacheConfiguration(name)
    }

    /**
     * Sensible defaults
     * @param name
     * @return
     */
    static CacheConfiguration getDefaultCacheConfiguration(String name) {
        def config = new CacheConfiguration(name)
        config.setCacheMode(CacheMode.PARTITIONED)
//        config.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED)
        config.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC)
//        config.setOffHeapMaxMemory(0)
//        config.setEvictionPolicy(new LruEvictionPolicy(10000))
        config.setBackups(0)
//        config.setSwapEnabled(false)
        config.setStatisticsEnabled(false)
        config.setManagementEnabled(false)
        config.setCopyOnRead(false)
//        config.setEvictSynchronized(true)

        return config
    }

    /**
     * Sensible defaults
     * @param name
     * @return
     */
    static CacheConfiguration getCacheConfigurationLru(String name,
                                                       CacheMode cacheMode,
//                                                       CacheMemoryMode cacheMemoryMode,
                                                       CacheAtomicityMode cacheAtomicityMode,
                                                       CacheWriteSynchronizationMode synchronizationMode,
//                                                       long offHeapMaxMemory,
                                                       int maxElements,
                                                       int backups,
//                                                       boolean swapEnabled,
                                                       boolean statisticsEnabled,
                                                       boolean managementEnabled,
                                                       boolean copyOnRead) {
//                                                       boolean evictSynchronized) {
        def config = new CacheConfiguration(name);
        config.setCacheMode(cacheMode)
//        config.setMemoryMode(cacheMemoryMode)
        config.setWriteSynchronizationMode(synchronizationMode)
        config.setAtomicityMode(cacheAtomicityMode)
//        config.setOffHeapMaxMemory(offHeapMaxMemory);
//        config.setEvictionPolicy(new LruEvictionPolicy(maxElements));
//        config.setSwapEnabled(swapEnabled)
        config.setStatisticsEnabled(statisticsEnabled)
        config.setManagementEnabled(managementEnabled)
        config.setBackups(backups)
        config.setCopyOnRead(copyOnRead)
//        config.setEvictSynchronized(evictSynchronized)

        return config
    }
}
