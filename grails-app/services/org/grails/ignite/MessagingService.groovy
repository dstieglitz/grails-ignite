package org.grails.ignite

import org.apache.ignite.IgniteMessaging
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.ignite.lang.IgniteCallable
import org.springframework.beans.factory.InitializingBean

/**
 * Support point-to-point and topic-based messaging throughout the cluster
 */
class MessagingService implements InitializingBean {

    public static final String QUEUE_DESTINATION_CACHE_NAME = '__queueDestinationCache'
    public static final long TIMEOUT = 30000
    static transactional = false

    def grid

    @Override
    public void afterPropertiesSet() {
        CacheConfiguration<String, MessageReceiver> cacheConf = new CacheConfiguration<>();
        cacheConf.setName(QUEUE_DESTINATION_CACHE_NAME)
        cacheConf.setCacheMode(CacheMode.PARTITIONED)
        cacheConf.setAtomicityMode(CacheAtomicityMode.ATOMIC)
        cacheConf.setBackups(1)
        grid.createCache(cacheConf)
    }

    /**
     * Send a message to a destination. This method emulates the old Grails JMS method of sending messages, e.g.,
     * <pre> sendMessage(queue:'queue_name', message) </pre>
     * <p>or</p>
     * <pre> sendMessage(topic: 'topic_name', message)
     */
    def sendMessage(destination, message) {
        log.debug "sendMessage(${destination},${message})"
        if (!(destination instanceof Map)) {
            throw new RuntimeException("Message destination must be of the form [type:name], e.g., [queue:'myQueue']")
        }

        if (destination.queue) {
            def queueName = destination.queue
            // execute the listener on the receiving node
            grid.compute().call(new IgniteCallable<Object>() {
                @Override
                public Object call() throws Exception {
                    // get a listener for this destination
                    def receiver = (MessageReceiver) grid.cache(QUEUE_DESTINATION_CACHE_NAME).get(queueName)
                    if (receiver == null) {
                        throw new RuntimeException("No receiver configured for queue ${destination.queue}")
                    }
                    // somehow execute the listener
                    receiver.receive(message)
                }
            });
        }

        if (destination.topic) {
            IgniteMessaging rmtMsg = grid.message();
            rmtMsg.sendOrdered(destination.topic, message, TIMEOUT)
        }
    }

    def registerListener(destination, MessageReceiver receiver) {
        log.debug "registerListener(${destination},${receiver})"
        if (!(destination instanceof Map)) {
            throw new RuntimeException("Message destination must be of the form [type:name], e.g., [queue:'myQueue']")
        }

        if (destination.queue) {
            grid.cache(QUEUE_DESTINATION_CACHE_NAME).put(destination.queue, receiver)
        }

        if (destination.topic) {
            IgniteMessaging rmtMsg = grid.message();
            def topicName = destination.topic
            rmtMsg.remoteListen(topicName, new IgniteBiPredicate<UUID, String>() {
                @Override
                public boolean apply(UUID nodeId, String msg) {
                    receiver.receive(msg)
                }
            });
        }
    }
}