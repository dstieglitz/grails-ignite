package org.grails.ignite

import grails.test.spock.IntegrationSpec
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.codehaus.groovy.grails.commons.GrailsApplication

class ConfigurationIntegrationSpec extends IntegrationSpec {

    def grid

    def setup() {
    }

    def cleanup() {
    }

    void "test cache configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        println grid.configuration()
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
}
