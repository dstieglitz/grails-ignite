package org.grails.ignite

import org.apache.ignite.cache.query.annotations.QueryTextField

class Widget {

    @QueryTextField
    String name

    static mapping = {
        cache true
    }

    static constraints = {
    }

    static ignite = true

//     TODO Index the name field as text
//    static ignite = {
//        name type:'text'
//    }

    String toString() {
        return "id=$id,name=$name"
    }
}
