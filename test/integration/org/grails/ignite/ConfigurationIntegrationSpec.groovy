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
        caches.containsKey('test_replicated')
        caches['test_replicated'].cacheMode == CacheMode.REPLICATED
        caches['test_replicated'].writeSynchronizationMode == CacheWriteSynchronizationMode.FULL_SYNC
        caches['test_replicated'].atomicityMode == CacheAtomicityMode.TRANSACTIONAL

        caches.containsKey('test_partitioned')
        caches['test_partitioned'].cacheMode == CacheMode.PARTITIONED
        caches['test_partitioned'].writeSynchronizationMode == CacheWriteSynchronizationMode.FULL_ASYNC
        caches['test_partitioned'].atomicityMode == CacheAtomicityMode.ATOMIC
    }

    void "test l2 cache configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        def caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }

        then:
        caches.containsKey('org.grails.ignite.Widget') // region configured manually

        when:
        def widget = new Widget(name: 'test widget')
        widget.save()
        sessionFactory.currentSession.clear()
        caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }
        println grid.configuration().cacheConfiguration.collect { it.name }
        widget = Widget.get(1)
        println widget
        println sessionFactory.statistics
        println sessionFactory.statistics.secondLevelCacheStatistics

        then:
        widget
        Widget.count() == 1
        grid.cache('org.grails.ignite.Widget').size() == 1
        caches.containsKey('org.grails.ignite.Widget')

        // caches configured programatically cannot be queried via the Ignite interface (until version 1.5)
    }
}
