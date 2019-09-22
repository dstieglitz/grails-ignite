package org.grails.ignite

import grails.gorm.transactions.Rollback
import grails.testing.mixin.integration.Integration
import org.apache.ignite.Ignite
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.CacheWriteSynchronizationMode
import org.grails.ignite.Widget
import org.junit.Rule
import org.springframework.boot.test.rule.OutputCapture
import spock.lang.Specification

@Integration
@Rollback
class ConfigurationIntegrationSpec extends Specification {

    def grid
    def sessionFactory

    @Rule
    OutputCapture capture = new OutputCapture()

    void "test ignite startup"() {
        setup:
        def testString

        when:
        testString = $/
>>>    __________  ________________  
>>>   /  _/ ___/ |/ /  _/_  __/ __/  
>>>  _/ // (7 7    // /  / / / _/    
>>> /___/\___/_/|_/___/ /_/ /___/   
>>> 
/$.replace("\r\n", "").replace("\n", "")

        then:
        def captureString = capture.toString().replace("\r\n", "").replace("\n", "")
        def testCount = captureString.count(testString)
        testCount == 1
        //println capture.toString()
    }

    void "test grid creation"() {
        when:
        grid

        then:
        grid.name() != null
        grid.underlyingIgnite != null
        grid instanceof Ignite
    }

    void "test cache configuration"() {
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