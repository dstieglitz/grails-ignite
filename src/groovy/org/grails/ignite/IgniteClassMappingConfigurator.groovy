package org.grails.ignite

import groovy.util.logging.Log4j
import org.apache.ignite.cache.QueryEntity
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty

/**
 * Created by dstieglitz on 11/21/16.
 */
@Log4j
class IgniteClassMappingConfigurator {
    static def queryEntityPrefix = "QE_"
    static def ignitePropertyName = "ignite"

    def grid
    def grailsApplication
    def includeTransients = true

    def configureAndInstallMappings() {
        // get ignite closure
        // if queryEntity = true
        // configure class for cache queries
        def domainClasses = grailsApplication.getArtefacts("Domain")
        domainClasses.each { domainClass ->
            Object searchable = domainClass.getPropertyValue(ignitePropertyName)
            if (searchable instanceof Boolean) {
                if (searchable) buildDefaultMapping(domainClass)
//            } else if (searchable instanceof java.util.LinkedHashMap) {
//                Set<String> inheritedProperties = getInheritedProperties(domainClass)
//                buildHashMapMapping((LinkedHashMap) searchable, domainClass, inheritedProperties)
//            } else if (searchable instanceof Closure) {
//                Set<String> inheritedProperties = getInheritedProperties(domainClass)
//                buildClosureMapping(domainClass, (Closure) searchable, inheritedProperties)
            } else if (searchable != null) {
                throw new IllegalArgumentException("'$ignitePropertyName' property has unknown type: " + searchable.getClass())
            }
        }
    }

    def buildDefaultMapping(domainClass) {
        // get cache configuration from config (if exists)
        def cacheName = "$queryEntityPrefix$domainClass.name"
        log.info "building cache mapping for ${cacheName}"
        if (!grid.cache(cacheName)) {
            def cc = IgniteCacheConfigurationFactory.getCacheConfiguration(cacheName)
            // FIXME configure later
//            cc.setWriteThrough(false)
//            cc.setReadThrough(true)
            log.info "configuring query entity $domainClass.name"
            def queryEntity = new QueryEntity(keyType: 'java.lang.Long', valueType: domainClass.name)
            // get mapped properties

            getDomainProperties(domainClass).each { prop ->
                log.info "configuring property $prop.name, $prop.type, null"
                queryEntity.addQueryField(prop.name, prop.typePropertyName, null)
            }

            cc.setQueryEntities([queryEntity])

            def cache = grid.getOrCreateCache(cc)
            log.info "configured cache ${cache}"
        }

        // create default QE cache
    }

    private GrailsDomainClassProperty[] getDomainProperties(GrailsDomainClass domainClass) {
        GrailsDomainClassProperty[] properties
        if (includeTransients) {
            properties = domainClass.getProperties()
            //These properties are specific to GORM and of no use for search. For backwards compatibility they are not included
            properties = properties - domainClass.getPropertyByName("id") - domainClass.getPropertyByName("version")
        } else {
            properties = domainClass.getPersistentProperties()
        }

        properties
    }
}
