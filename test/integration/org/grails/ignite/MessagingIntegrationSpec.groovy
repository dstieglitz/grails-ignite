package org.grails.ignite

import grails.test.spock.IntegrationSpec

class MessagingIntegrationSpec extends IntegrationSpec {

    def messagingService

    def setup() {
    }

    def cleanup() {
    }

    void "test something"() {
        setup:
        def exceptionThrown = false

        when:
        messagingService.registerListener(queue: 'hello', new ExpressionEvaluatingMessageReceiver('println'))
        messagingService.sendMessage(queue: 'hello', "world")
        messagingService.sendMessage(queue: 'hello', "goodbye")

        messagingService.registerListener(topic: 'hello', new ExpressionEvaluatingMessageReceiver('iGotTheMessage'))
        messagingService.sendMessage(topic: 'hello', "world")

        try {// will throw exception
            messagingService.sendMessage(queue: 'noreceiver', "goodbye")
        } catch (RuntimeException r) {
            exceptionThrown = true
        }

        then:
        exceptionThrown
    }

    private boolean iGotTheMessage(message) {
        println "iGotTheMessage: ${message}"
    }
}
