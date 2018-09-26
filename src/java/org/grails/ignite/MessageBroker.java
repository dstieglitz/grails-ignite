package org.grails.ignite;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Interface for a distributed message broker
 */
public interface MessageBroker {
    void sendMessage(Map destinationData, Object message) throws MessageBrokerException;

    Future sendMessageAsync(Map destinationData, Object message);

    void registerReceiver(Map destinationData, MessageReceiver receiver) throws MessageBrokerException;
}
