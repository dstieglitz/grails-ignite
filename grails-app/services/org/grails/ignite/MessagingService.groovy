package org.grails.ignite

import org.apache.ignite.IgniteMessaging
import static grails.async.Promises.*
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import org.springframework.beans.factory.InitializingBean

/**
 * Support point-to-point and topic-based messaging throughout the cluster
 */
class MessagingService implements InitializingBean {

    public static final String QUEUE_DESTINATION_CACHE_NAME = '__queueDestinationCache'
    public static final long TIMEOUT = 30000
    static transactional = false

    def grid
    // a local cache of receivers that will be assigned messages upon arrival at this node. It's
    // not always feasable to serialize message receivers
    //private Map localReceiverCache = new HashMap<String, MessageReceiver>()

    @Override
    public void afterPropertiesSet() {
        // for testing where no grid is available, need to be able to initialize this bean
        if (grid == null) {
            //           throw new RuntimeException("Can't configure messaging, no grid is configured")
            log.warn "Can't configure messaging, no grid is configured"
        } else {
            CacheConfiguration<String, MessageReceiver> cacheConf = new CacheConfiguration<>();
            cacheConf.setName(QUEUE_DESTINATION_CACHE_NAME)
            cacheConf.setCacheMode(CacheMode.PARTITIONED)
            cacheConf.setAtomicityMode(CacheAtomicityMode.ATOMIC)
            cacheConf.setBackups(0)
            grid.getOrCreateCache(cacheConf)
            log.debug "afterPropertiesSet --> configured cache ${cacheConf}"
        }
    }

    /**
     * Send a message to a destination synchronously. This method emulates the old Grails JMS method of sending messages, e.g.,
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
            grid.compute().call(new IgniteMessagingQueueReceiverWrapper(grid, queueName, destination, message));
        }

        if (destination.topic) {
            log.debug "sending to topic: ${destination.topic}, ${message}, with timeout=${TIMEOUT}"
            IgniteMessaging rmtMsg = grid.message();
            rmtMsg.sendOrdered(destination.topic, message, TIMEOUT)
        }
    }

    /**
     * Send a message to a destination asnychronously. This method emulates the old Grails JMS method of sending messages, e.g.,
     * <pre> sendMessage(queue:'queue_name', message) </pre>
     * <p>or</p>
     * <pre> sendMessage(topic: 'topic_name', message)
     * Returns an object you can listen to:
     * <pre>
     * // Get the future for the above invocation.
     * IgniteFuture<String> fut = asyncCompute.future();
     *
     * // Asynchronously listen for completion and print out the result.
     * fut.listen(f -> System.out.println("Job result: " + f.get()))
     </pre>
     @see http://apacheignite.gridgain.org/docs/async-support
     */
    def sendMessageAsync(destination, message) {
        log.debug "sendMessageAsync(${destination},${message})"
        if (!(destination instanceof Map)) {
            throw new RuntimeException("Message destination must be of the form [type:name], e.g., [queue:'myQueue']")
        }

        if (destination.queue) {
            def queueName = destination.queue
            // execute the listener on the receiving node
            def asyncCompute = grid.compute().withAsync()
            asyncCompute.call(new IgniteMessagingQueueReceiverWrapper(grid, queueName, destination, message))
            def future = asyncCompute.future();
            log.debug("got future ${future}")
            return future;
        }

        if (destination.topic) {
            log.debug "sending to topic: ${destination.topic}, ${message}, with timeout=${TIMEOUT}"
            task {
                IgniteMessaging rmtMsg = grid.message();
                rmtMsg.send(destination.topic, message)
            }
            def future = rmtMsg.future()
            log.debug("got future ${future}")
            return future
        }
    }

    def registerReceiver(destination, MessageReceiver receiver) {
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
            rmtMsg.remoteListen(topicName, new IgniteMessagingRemoteListener(receiver, destination));
        }
    }
}
