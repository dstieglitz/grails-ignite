package org.grails.ignite

import grails.test.mixin.TestFor
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.query.QueryCursor
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.cache.query.TextQuery
import org.apache.ignite.lang.IgniteBiPredicate
import spock.lang.Specification

import java.util.Map.Entry

/**
 * See the API for {@link grails.test.mixin.services.ServiceUnitTestMixin} for usage instructions
 */
@TestFor(QueryEntityService)
class QueryEntityServiceSpec extends Specification {

    def grid
    def widget1, widget2, widget3

    def setup() {
    }

    def cleanup() {
    }

    def createSomeWidgets() {
        widget1 = new Widget(name: 'Harry Potter')
        widget1.name = 'Harry Potter'
        if (!widget1.save()) {
            widget1.errors.each {
                println it
            }
        }
        println "created widget ${widget1}"

        widget2 = new Widget()
        widget2.name = 'Harry Potter'
        if (!widget2.save()) {
            widget2.errors.each {
                println it
            }
        }
        println "created widget ${widget2}"

        widget3 = new Widget()
        widget3.name = 'Hermione Grainger'
        if (!widget3.save()) {
            widget3.errors.each {
                println it
            }
        }
        println "created widget ${widget3}"
    }

    void "test query entity configuration"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null

        when:
        true

        then:
        grid.cacheNames().contains('QE_Widget')
    }

    void "test query entity"() {
        setup:
        assert grid.name() != null // force creation of grid
        assert grid.underlyingIgnite != null
        createSomeWidgets()

        when:
        true

        then:
        grid.cacheNames().contains('QE_Widget')

        when:
        grid.cache('QE_Widget').put(widget1.id, widget1)

        then:
        grid.cache('QE_Widget').size() == 1

        when:
        grid.cache('QE_Widget').put(widget2.id, widget2)

        then:
        grid.cache('QE_Widget').size() == 2

        when:
        grid.cache('QE_Widget').put(widget3.id, widget3)

        then:
        grid.cache('QE_Widget').size() == 3

        when: "i'm testing scan queries"
        IgniteCache<Long, Widget> cache = grid.cache("QE_Widget");
        IgniteBiPredicate<Long, Widget> filter = new IgniteBiPredicate<Long, Widget>() {
            @Override
            public boolean apply(Long key, Widget p) {
                println "${key},${p}"
                return p.name.equals('Harry Potter')
            }
        };

        QueryCursor cursor = cache.query(new ScanQuery(filter));

        then:
        cursor.size() == 2

        when: "i'm testing text queries"
        cache = grid.cache("QE_Widget");

        // Query for all people with "Master Degree" in their resumes.
        TextQuery txt = new TextQuery(Widget.class, "Master Degree");
        QueryCursor<Entry<Long, Widget>> results = cache.query(txt);

        then:
        results.size() == 0

        when: "i'm testing text queries"
        cache = grid.cache("QE_Widget");

        txt = new TextQuery(Widget.class, 'Hermione Grainger');
        results = cache.query(txt);
//        println results.all
//        println cache.get(widget3.id).name

        then:
        results.size() == 1
    }

    void "test loadCache"() {
        setup:
        grid.cache('QE_Widget').clear()

        when:
        createSomeWidgets()

        then:
        grid.cache('QE_Widget').size() == 0

        when:
        grid.cache('QE_Widget').loadCache(null)

        then:
        grid.cache('QE_Widget').size() == 3
    }

    void "test writeBehind"() {
        setup:
        grid.cache('QE_Widget').clear()

        when:
        createSomeWidgets()

        then:
        grid.cache('QE_Widget').size() == 0

        when: "I try to write an entity through"
        def widget4 = new Widget()
        widget4.name = 'Ron Weasley'
        println "created widget ${widget4}"

        // FIXME don't know ID yet!
        grid.cache('QE_Widget').put(-1, widget4)

        then:
        grid.cache('QE_Widget').size() == 1

        when: "i try to read and entity through"
        def gotWidget = grid.cache('QE_Widget').get(widget2.id)

        then:
        grid.cache('QE_Widget').size() == 2
        gotWidget.name == 'Harry Potter'
    }
}
