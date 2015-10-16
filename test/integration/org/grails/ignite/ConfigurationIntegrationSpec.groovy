package org.grails.ignite

import grails.test.spock.IntegrationSpec
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode

class ConfigurationIntegrationSpec extends IntegrationSpec {

    def grid
    def sessionFactory

    void "test cache configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        def caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }

        then:
        assert caches.containsKey('test_replicated')
        assert caches['test_replicated'].cacheMode == CacheMode.REPLICATED
        assert caches['test_replicated'].writeSynchronizationMode == CacheWriteSynchronizationMode.FULL_SYNC
        assert caches['test_replicated'].atomicityMode == CacheAtomicityMode.TRANSACTIONAL

        assert caches.containsKey('test_partitioned')
        assert caches['test_partitioned'].cacheMode == CacheMode.PARTITIONED
        assert caches['test_partitioned'].writeSynchronizationMode == CacheWriteSynchronizationMode.FULL_ASYNC
        assert caches['test_partitioned'].atomicityMode == CacheAtomicityMode.ATOMIC
    }

    void "test l2 cache configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        def caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }

        then:
        assert caches.containsKey('org.grails.ignite.Widget') // region configured manually

        when:
        def widget = new Widget(name: 'test widget')
        widget.save(flush: true)
        caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }
        println grid.configuration().cacheConfiguration.collect { it.name }
        widget = Widget.get(1)

        then:
        Widget.count() == 1
        assert grid.cache('org.grails.ignite.Widget').metrics().size == 1
        assert caches.containsKey('org.grails.ignite.Widget')
        println sessionFactory.statistics
        println sessionFactory.statistics.secondLevelCacheStatistics

        // caches configured programatically cannot be queried via the Ignite interface (until version 1.5)
    }
}
