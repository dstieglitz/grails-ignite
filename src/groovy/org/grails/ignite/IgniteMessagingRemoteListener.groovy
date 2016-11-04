package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.lang.IgniteBiPredicate
import org.codehaus.groovy.grails.support.PersistenceContextInterceptor
import org.springframework.beans.factory.NoSuchBeanDefinitionException

/**
 * Created by dstieglitz on 10/29/16.
 */
@Log4j
public class IgniteMessagingRemoteListener implements IgniteBiPredicate<UUID, Object>, Serializable {
    private Object destination;
    private MessageReceiver receiver;

    public IgniteMessagingRemoteListener(MessageReceiver receiver, Object destination) {
        this.destination = destination;
        this.receiver = receiver;
    }

    @Override
    public boolean apply(UUID nodeId, Object msg) {
        log.debug "apply ${nodeId}, ${msg}"
        def ctx = Holders.applicationContext
        PersistenceContextInterceptor persistenceInterceptor = null

        try {
            persistenceInterceptor = (PersistenceContextInterceptor) ctx.getBean("persistenceInterceptor");
            persistenceInterceptor.init()
            log.trace "initialized persistence interceptor"
            receiver.receive(destination, msg);
            return true;
        } catch (NoSuchBeanDefinitionException e) {
            log.warn "${e.message}; aborting job"
        } finally {
            log.trace "flushing/destroying persistence interceptor"
            if (persistenceInterceptor != null) {
                persistenceInterceptor.flush()
                persistenceInterceptor.destroy()
            }
        }
    }
}
