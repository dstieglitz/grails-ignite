package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j

/**
 * Created by dstieglitz on 10/27/16.
 */
@Log4j
class SpringBeanRefMessageReceiver implements MessageReceiver {

    def messageReceiverBeanName

    public SpringBeanRefMessageReceiver(beanName) {
        this.messageReceiverBeanName = beanName
    }

    @Override
    void receive(Object destination, Object data) {
        // look up application context
        log.debug "receive ${destination}, ${data}"
        def beanRef = Holders.applicationContext.getBean(this.messageReceiverBeanName)
        log.debug "got bean ${beanRef}"
        if (!beanRef) {
            log.warn "no receiving bean found with ref ${messageReceiverBeanName}"
        } else {
            beanRef.receive(destination, data);
        }
    }
}
