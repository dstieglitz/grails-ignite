package org.grails.ignite

import groovy.util.logging.Log4j
import org.apache.ignite.cache.QueryIndex
import org.apache.ignite.cache.QueryIndexType
import org.codehaus.groovy.grails.commons.GrailsClassUtils
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.codehaus.groovy.grails.exceptions.InvalidPropertyException

import javax.cache.configuration.FactoryBuilder

/**
 * This class is invoked by the spring context and configures the Ignite class. The methodology and some code are borrowed
 * from the Searchable grails plugin. HT the authors.
 * @see SearchableDomainClassMapper
 * Created by dstieglitz on 11/21/16.
 *
 * @deprecated this class is left here as documentation but does not work. Use the ignite annotations on domain classes
 * to configure the indices on your grid
 */
@Log4j
class IgniteClassMappingConfigurator {
    static def queryEntityPrefix = "QE_"
    static def ignitePropertyName = "ignite"

    def grid
    def grailsApplication
    def includeTransients = true
    def customMappedProperties = [:]

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
            } else if (searchable instanceof Closure) {
                Set<String> inheritedProperties = getInheritedProperties(domainClass)
                buildClosureMapping(domainClass, (Closure) searchable, inheritedProperties)
            } else if (searchable != null) {
                throw new IllegalArgumentException("'$ignitePropertyName' property has unknown type: " + searchable.getClass())
            }
        }
    }

    void buildClosureMapping(GrailsDomainClass grailsDomainClass, Closure searchable, Set<String> inheritedProperties) {
        log.info "buildClosureMapping $grailsDomainClass, $searchable, $inheritedProperties"
        assert searchable != null

        // Build user-defined specific mappings
        Closure closure = (Closure) searchable.clone()
        closure.setDelegate(this)
        closure.call()

//        buildMappingFromOnlyExcept(grailsDomainClass, inheritedProperties)
        buildMappingFromClosure(grailsDomainClass, inheritedProperties)
    }

    private Set<String> getInheritedProperties(GrailsDomainClass domainClass) {
        // check which properties belong to this domain class ONLY
        Set<String> inheritedProperties = []
        for (GrailsDomainClassProperty prop : getDomainProperties(domainClass)) {
            if (GrailsClassUtils.isPropertyInherited(domainClass.getClazz(), prop.getName())) {
                inheritedProperties.add(prop.getName())
            }
        }
        return inheritedProperties
    }

    /**
     * Invoked by 'searchable' closure.
     *
     * @param name synthetic method name
     * @param args method arguments.
     * @return <code>null</code>
     */
    def invokeMethod(String name, args) {
        log.info "invokeMethod $name, $args"
//        // Custom properties mapping options
//        GrailsDomainClassProperty property = grailsDomainClass.getPropertyByName(name)
//        Assert.notNull(property, "Unable to find property [$name] used in [$grailsDomainClass.propertyName]#${getSearchablePropertyName()}].")
//
        if (!(args.first() instanceof Map)) throw new RuntimeException("Args must take the form name:value for ignite field properties")

//        // Check if we already has mapping for this property.
        def propertyMapping = customMappedProperties.get(name)
        if (propertyMapping == null) {
//            propertyMapping = new SearchableClassPropertyMapping(property)
            customMappedProperties.put(name, args.first())
        }
//        //noinspection unchecked
//        propertyMapping.addAttributes((Map<String, Object>) ((Object[]) args)[0])
        return null
    }

    def buildMappingFromClosure(grailsDomainClass, inheritedProperties) {
        // get cache configuration from config (if exists)
        def cacheName = "$queryEntityPrefix$grailsDomainClass.name"
        log.info "building cache mapping for ${cacheName}"

        if (!grid.cache(cacheName)) {
            def cc = IgniteCacheConfigurationFactory.getCacheConfiguration(cacheName)

            def idxs = []
            def keyType = grailsDomainClass.getPropertyByName('id').type
            def valueType = Class.forName(grailsDomainClass.fullName)

            // use annotations instead
//            log.info "configuring query entity keyType:$keyType.name, valueType: $valueType.name"
//            def queryEntity = new QueryEntity(keyType: keyType.name, valueType: valueType.name)

            // get mapped properties
            customMappedProperties.each { kvp ->
                String propName = kvp.key
                Map mapping = kvp.value as Map
                log.debug "configuring custom mapped properties ${propName} with $mapping"

                try {
                    String propType = grailsDomainClass.getPropertyByName(propName).type.name
                    log.info "configuring property $propName, $propType, $mapping"
                } catch (InvalidPropertyException e) {
                    log.debug e.message, e
                }

//                if (!mapping.type) throw new RuntimeException("No type specified for field $propName in ignite mapping")
//                idxs.addAll configureQueryIndices(grailsDomainClass, queryEntity, mapping, propName, propType)
                if (propName.equals('cacheStore')) {
                    if (mapping.enabled) {
                        log.info "configuring cache store factory for ${valueType.name}"
                        def cacheStore = new GrailsDomainCacheStore(valueType)
                        cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(cacheStore))
                    }
                    if (mapping.readThrough) {
                        cc.setReadThrough(true)
                    }
                    if (mapping.writeThrough) {
                        cc.setWriteThrough(true)
                    }
                }
            }

            cc.setIndexedTypes(keyType, valueType)
//            cc.setQueryEntities([queryEntity])

//            if (!idxs.empty) {
//                queryEntity.setIndexes(idxs)
//                log.info "added $idxs.size fulltext index(es) for class $grailsDomainClass.fullName"
//            }

            def cache = grid.getOrCreateCache(cc)
            log.info "configured cache ${cache}"
        }

        // create default QE cache
    }

    def buildDefaultMapping(grailsDomainClass) {
        // get cache configuration from config (if exists)
        def cacheName = "$queryEntityPrefix$grailsDomainClass.name"
        log.info "building cache mapping for ${cacheName}"
        def keyType = grailsDomainClass.getPropertyByName('id').type
        def valueType = Class.forName(grailsDomainClass.fullName)

        if (!grid.cache(cacheName)) {
            def cc = IgniteCacheConfigurationFactory.getCacheConfiguration(cacheName)
            // FIXME configure later
//            cc.setWriteThrough(false)
//            cc.setReadThrough(true)
            log.info "configuring query entity $grailsDomainClass.name"

            // use annotations instead
//            def queryEntity = new QueryEntity(keyType: 'java.lang.Long', valueType: grailsDomainClass.name)
            // get mapped properties

//            getDomainProperties(domainClass).each { prop ->
//                log.info "configuring property $prop.name, $prop.type, null"
//                queryEntity.addQueryField(prop.name, prop.typePropertyName, null)
//            }

            cc.setIndexedTypes(keyType, valueType)
            //           cc.setQueryEntities([queryEntity])

            def cache = grid.getOrCreateCache(cc)
            log.info "configured cache ${cache}"
        }

        // create default QE cache
    }

    /**
     * @deprecated does not work use annotations instead (or fix)
     * @param grailsDomainClass
     * @param queryEntity
     * @param mapping
     * @param propName
     * @param propType
     * @return
     */
    def configureQueryIndices(grailsDomainClass, queryEntity, mapping, propName, propType) {
        def idxs = []
        queryEntity.addQueryField(propName, propType, null)
        if (mapping.type.equalsIgnoreCase('text')) {
            def txtIdx = new QueryIndex();
            txtIdx.setIndexType(QueryIndexType.FULLTEXT);
            txtIdx.setFieldNames([propName], true)
            txtIdx.setName("$queryEntityPrefix$grailsDomainClass.name$propName")
            idxs.add(txtIdx)
            log.info "created FULLTEXT index for property $propName: $txtIdx"
        }

        return idxs
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
