package org.grails.ignite

import grails.test.spock.IntegrationSpec
import org.apache.ignite.cache.CacheMode
import org.codehaus.groovy.grails.commons.GrailsApplication

class ConfigurationIntegrationSpec extends IntegrationSpec {

    GrailsApplication grailsApplication

    def setup() {
        IgniteStartupHelper.startIgnite()
    }

    def cleanup() {
    }

    void "test cache modes"() {
        setup:
        def grid = grailsApplication.mainContext.getBean('grid')
        assert grid != null

        when:
        println grid.configuration().cacheConfiguration
        def caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it.mode] }.flatten()

        then:
        assert caches.containsKey('test_replicated')
        assert caches['test_replicated'] == CacheMode.REPLICATED
    }
}
