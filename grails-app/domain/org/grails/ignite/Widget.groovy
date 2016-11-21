package org.grails.ignite

class Widget {

    def name

    static mapping = {
        cache true
    }

    static constraints = {
    }

    static ignite = true
}
