package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j
import org.apache.ignite.Ignite
import org.apache.ignite.lang.IgniteCallable
import org.codehaus.groovy.grails.support.PersistenceContextInterceptor
import org.springframework.beans.factory.NoSuchBeanDefinitionException

/**
 * @deprecated this class was used by the old MessagingService. The MessageBroker paradigm uses a different methodology
 * @see MessageBroker, MessageBrokerImpl
 * Created by dstieglitz on 11/4/16.
 */
@Log4j
class IgniteMessagingQueueReceiverWrapper implements IgniteCallable<Object> {

    private Ignite grid;
    private String queueName;
    private Object destination;
    private Object message;

    public IgniteMessagingQueueReceiverWrapper(Ignite grid,
                                               String queueName,
                                               Object destination,
                                               Object message) {
        this.grid = grid;
        this.queueName = queueName;
        this.destination = destination;
        this.message = message;
    }

    @Override
    public Object call() throws Exception {
        log.debug "call ${destination}, ${message}"
        def ctx = Holders.applicationContext
        PersistenceContextInterceptor persistenceInterceptor = null

        try {
            persistenceInterceptor = (PersistenceContextInterceptor) ctx.getBean("persistenceInterceptor");
            persistenceInterceptor.init()
            // get a listener for this destination
            def receiver = (MessageReceiver) grid.cache(MessagingService.QUEUE_DESTINATION_CACHE_NAME).get(queueName)
            if (receiver == null) {
//                        throw new RuntimeException("No receiver configured for queue ${destination.queue}")
                // suppress warnings?
                log.warn "No receiver configured for queue ${destination.queue}"
            } else {
                receiver.receive(destination, message)
            }
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

