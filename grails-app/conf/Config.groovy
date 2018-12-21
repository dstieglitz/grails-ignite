// configuration for plugin testing - will not be included in the plugin zip

ignite.enabled = true
ignite.gridName = 'grid'
ignite.config.locations = ['file:ignite/conf/IgniteResources.groovy','file:ignite/conf/IgniteCacheResources.groovy']
ignite.l2CacheEnabled = true
ignite.discoverySpi.multicastDiscovery = true
ignite.peerClassLoadingEnabled = true

log4j = {
    // Example of changing the log pattern for the default console
    // appender:
    //
    //appenders {
    //    console name:'stdout', layout:pattern(conversionPattern: '%c{2} %m%n')
    //}

    trace 'groovy.lang.GroovyClassLoader'
    debug 'grails.app.services'

    error  'org.codehaus.groovy.grails.web.servlet',  //  controllers
           'org.codehaus.groovy.grails.web.pages', //  GSP
           'org.codehaus.groovy.grails.web.sitemesh', //  layouts
           'org.codehaus.groovy.grails.web.mapping.filter', // URL mapping
           'org.codehaus.groovy.grails.web.mapping', // URL mapping
           'org.codehaus.groovy.grails.commons', // core / classloading
           'org.codehaus.groovy.grails.plugins', // plugins
           'org.codehaus.groovy.grails.orm.hibernate', // hibernate integration
           'org.springframework'
    error  'org.hibernate',
           'net.sf.ehcache.hibernate'

    debug 'org.apache.ignite'
    debug 'grails.plugins.ignite'
}
