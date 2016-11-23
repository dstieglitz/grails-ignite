package org.grails.ignite

import groovy.util.logging.Log4j
import org.apache.ignite.cache.store.CacheStore
import org.apache.ignite.cache.store.CacheStoreAdapter
import org.apache.ignite.lang.IgniteBiInClosure
import org.jetbrains.annotations.Nullable

import javax.cache.Cache
import javax.cache.integration.CacheLoaderException
import javax.cache.integration.CacheWriterException

/**
 * Created by dstieglitz on 11/22/16.
 */
@Log4j
class GrailsDomainCacheStore extends CacheStoreAdapter implements CacheStore, Serializable {

    Class grailsDomainClass

    public GrailsDomainCacheStore(Class domainClass) {
        this.grailsDomainClass = domainClass;
    }

    @Override
    void loadCache(IgniteBiInClosure clo, @Nullable Object... objects) throws CacheLoaderException {
        log.info "loadCache ${clo} ${objects}"
        // need name of objec to load
        grailsDomainClass.list().each {
            log.debug "loading ${it}"
            clo.apply(it.id, it)
        }
    }

    @Override
    void sessionEnd(boolean b) throws CacheWriterException {
        println "sessionEnd: ${b}"
    }

    @Override
    Object load(Object o) throws CacheLoaderException {
        return o.class.get(o as Long)
    }

    @Override
    void write(Cache.Entry entry) throws CacheWriterException {
        if (!entry.save()) {
            throw new CacheWriterException(entry.errors)
        }
    }

    @Override
    void delete(Object o) throws CacheWriterException {
        println "delete"
    }
}
