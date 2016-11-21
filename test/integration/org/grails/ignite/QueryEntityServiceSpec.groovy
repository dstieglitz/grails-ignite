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

    def setup() {
    }

    def cleanup() {
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

        when:
        true

        then:
        grid.cacheNames().contains('QE_Widget')

        when:
        def widget = new Widget(name: 'Harry Potter')
        if (!widget.save()) {
            widget.errors.each {
                println it
            }
        }
        grid.cache('QE_Widget').put(1l, widget)

        then:
        grid.cache('QE_Widget').size() == 1

        when:
        widget = new Widget(name: 'Harry Potter')
        if (!widget.save()) {
            widget.errors.each {
                println it
            }
        }
        grid.cache('QE_Widget').put(1l, widget)

        then:
        grid.cache('QE_Widget').size() == 1

        when:
        widget = new Widget(name: 'Hermione Grainger')
        if (!widget.save()) {
            widget.errors.each {
                println it
            }
        }
        grid.cache('QE_Widget').put(2l, widget)

        then:
        grid.cache('QE_Widget').size() == 2

        when: "i'm testing scan queries"
        IgniteCache<Long, Widget> cache = grid.cache("QE_Widget");
        IgniteBiPredicate<Long, Widget> filter = new IgniteBiPredicate<Long, Widget>() {
            @Override
            public boolean apply(Long key, Widget p) {
                println p.name
                return p.name.equals('Harry Potter')
            }
        };

        QueryCursor cursor = cache.query(new ScanQuery(filter));

        then:
        cursor.size() == 1

        when: "i'm testing text queries"
        cache = grid.cache("QE_Widget");

        // Query for all people with "Master Degree" in their resumes.
        TextQuery txt = new TextQuery(Widget.class, "Master Degree");
        QueryCursor<Entry<Long, Widget>> results = cache.query(txt);

        then:
        results.size() == 0

        when: "i'm testing text queries"
        cache = grid.cache("QE_Widget");

        // Query for all people with "Master Degree" in their resumes.
        txt = new TextQuery(Widget.class, "Harry Potter");
        results = cache.query(txt);

        then:
        results.size() == 1

    }
}
