package org.grails.ignite;

import grails.util.Holders;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Logger;
import org.codehaus.groovy.grails.support.PersistenceContextInterceptor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MessageBrokerImpl implements Service, MessageBroker {
    private static final Logger log = Logger.getLogger(MessageBrokerImpl.class.getName());
    private static final String QUEUE_DESTINATION_CACHE_NAME = "__queueDestinationCache";
    private static final String MESSAGE_QUEUE_NAME = "__messageQueue";
    private static final long TIMEOUT = 30000;

    @IgniteInstanceResource
    private Ignite ignite;
    private IgniteQueue<MessageWrapperFuture> messageQueue;

    @Override
    public void cancel(ServiceContext serviceContext) {

    }

    @Override
    public void init(ServiceContext serviceContext) throws Exception {
        CacheConfiguration<String, MessageReceiver> cacheConf = new CacheConfiguration<String, MessageReceiver>();
        cacheConf.setName(QUEUE_DESTINATION_CACHE_NAME);
        cacheConf.setCacheMode(CacheMode.REPLICATED);
        cacheConf.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheConf.setBackups(0);
        ignite.getOrCreateCache(cacheConf);
        log.debug("configured cache " + cacheConf);

        CollectionConfiguration colCfg = new CollectionConfiguration();
        colCfg.setCacheMode(CacheMode.REPLICATED);
        colCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        this.messageQueue = ignite.queue(MESSAGE_QUEUE_NAME, 0, colCfg);
    }

    @Override
    public void execute(ServiceContext serviceContext) throws Exception {
        // Loop until service is cancelled.
        while (!serviceContext.isCancelled()) {
            if (!messageQueue.isEmpty()) {
                MessageWrapperFuture wrapper = messageQueue.take();
                consumeFromQueue(wrapper);
                wrapper.consumed();
            }

            Thread.sleep(200);
        }
    }

    private void consumeFromQueue(MessageWrapper wrapper) {
        log.debug("consumeFromQueue " + wrapper.getDestination() + ", " + wrapper.getMessage() + ")");
        ApplicationContext ctx = Holders.getApplicationContext();
        PersistenceContextInterceptor persistenceInterceptor = null;
        String queueName = (String) wrapper.getDestination().get("queue");

        try {
            persistenceInterceptor = (PersistenceContextInterceptor) ctx.getBean("persistenceInterceptor");
            persistenceInterceptor.init();
            // get a listener for this destination
            MessageReceiver receiver = (MessageReceiver) ignite.cache(QUEUE_DESTINATION_CACHE_NAME).get(queueName);
            if (receiver == null) {
                log.warn("No receiver configured for queue '" + queueName + "'");
            } else {
                receiver.receive(wrapper.getDestination(), wrapper.getMessage());
            }
        } catch (NoSuchBeanDefinitionException e) {
            log.warn(e.getMessage());
        } finally {
            log.trace("flushing/destroying persistence interceptor");
            if (persistenceInterceptor != null) {
                persistenceInterceptor.flush();
                persistenceInterceptor.destroy();
            }
        }
    }

    /**
     * Send a message to a destination synchronously. This method emulates the old Grails JMS method of sending messages, e.g.,
     * <pre> sendMessage(queue:'queue_name', message) </pre>
     * <p>or</p>
     * <pre> sendMessage(topic: 'topic_name', message)
     */
    @Override
    public void sendMessage(Map destinationData, Object message) throws MessageBrokerException {
        log.debug("sendMessage(" + destinationData + "," + message + ")");

        if (destinationData.containsKey("queue")) {
//            String queueName = (String) destinationData.get("queue");
            // execute the listener on the receiving node
//            grid.compute().call(new IgniteMessagingQueueReceiverWrapper(grid, queueName, destination, message));
//            messageQueue.put(new MessageWrapper(queueName, (Serializable) message));
            consumeFromQueue(new MessageWrapper(destinationData, (Serializable) message));
        }

        if (destinationData.containsKey("topic")) {
            log.debug("sending to topic: " + destinationData.get("topic") + "," + message + ", with timeout=" + TIMEOUT);
            ignite.message().sendOrdered(destinationData.get("topic"), message, TIMEOUT);
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
     * </pre>
     *
     * @see http://apacheignite.gridgain.org/docs/async-support
     */
    @Override
    public Future sendMessageAsync(Map destinationData, Object message) {
//        log.warn("All message sending is asynchronous now");
        log.debug("sendMessageAsync(" + destinationData + "," + message + ")");

        if (destinationData.containsKey("queue")) {
            String queueName = (String) destinationData.get("queue");
            // execute the listener on the receiving node
//            grid.compute().call(new IgniteMessagingQueueReceiverWrapper(grid, queueName, destination, message));
            MessageWrapperFuture f = new MessageWrapperFuture(destinationData, (Serializable) message);
            messageQueue.put(f);
            return f;
        } else if (destinationData.containsKey("topic")) {
            String topicName = (String) destinationData.get("topic");
            MessageWrapperFuture f = new MessageWrapperFuture(destinationData, (Serializable) message);
            log.debug("async sending to topic: " + destinationData.get("topic") + "," + message + ", with timeout=" + TIMEOUT);
            ignite.message().sendOrdered(destinationData.get("topic"), message, TIMEOUT);
            f.consumed();
            return f;
        }

        return null;
    }

    @Override
    public void registerReceiver(Map destinationData, MessageReceiver receiver) throws MessageBrokerException {
        log.debug("registerListener(" + destinationData + "," + receiver + ")");

        if (destinationData.containsKey("queue")) {
            String queueName = (String) destinationData.get("queue");
            ignite.cache(QUEUE_DESTINATION_CACHE_NAME).put(queueName, receiver);
        }

        if (destinationData.containsKey("topic")) {
            IgniteMessaging rmtMsg = ignite.message();
            String topicName = (String) destinationData.get("topic");
            rmtMsg.localListen(topicName, new IgniteMessagingRemoteListener(receiver, destinationData));
        }
    }

    private class MessageWrapper implements Serializable {
        private Map destination;
        private Serializable message;

        public MessageWrapper(Map destination, Serializable message) {
            this.destination = destination;
            this.message = message;
        }

        public Map getDestination() {
            return this.destination;
        }

        public Serializable getMessage() {
            return this.message;
        }
    }

    private class MessageWrapperFuture extends MessageWrapper implements Future<Void> {
        private final CountDownLatch latch = new CountDownLatch(1);

        public MessageWrapperFuture(Map destination, Serializable message) {
            super(destination, message);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return latch.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException {
            latch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            if (latch.await(timeout, unit)) {
                return null;
            } else {
                throw new TimeoutException();
            }
        }

        // calling this more than once doesn't make sense, and won't work properly in this implementation. so: don't.
        void consumed() {
            latch.countDown();
        }
    }
}
