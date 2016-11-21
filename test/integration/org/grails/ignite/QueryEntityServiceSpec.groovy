package org.grails.ignite

import grails.test.mixin.TestFor
import spock.lang.Specification

/**
 * See the API for {@link grails.test.mixin.services.ServiceUnitTestMixin} for usage instructions
 */
@TestFor(QueryEntityService)
class QueryEntityServiceSpec extends Specification {

    def setup() {
    }

    def cleanup() {
    }

    void "test query entity configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        def caches = grid.configuration().cacheConfiguration.collectEntries { [(it.name): it] }

        then:
        caches.containsKey('QE_Widget')
    }
}
