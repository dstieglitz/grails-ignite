package org.grails.ignite


import org.springframework.beans.factory.InitializingBean

/**
 * Support point-to-point and topic-based messaging throughout the cluster
 */
class MessagingService {

    static transactional = false

    def grid
    
    def sendMessage(destination, message) {
        getServiceProxy().sendMessage(destination, message)
    }
    
    def sendMessageAsync(destination, message) {
        getServiceProxy().sendMessageAsync(destination, message)
    }

    def registerReceiver(destination, MessageReceiver receiver) {
        getServiceProxy().registerReceiver(destination, receiver)
    }

    private MessageBroker getServiceProxy() {
        if (grid == null) {
            throw new RuntimeException("Grid has not been initialized")
        }
        return grid.services().serviceProxy("messageBrokerService", MessageBroker.class, false)
    }
}
