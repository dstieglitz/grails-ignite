package org.grails.ignite

import grails.test.spock.IntegrationSpec
import org.junit.Rule
import org.springframework.boot.test.rule.OutputCapture

class MessagingIntegrationSpec extends IntegrationSpec {

    def messagingService

    @Rule
    OutputCapture capture = new OutputCapture()

    def setup() {
    }

    def cleanup() {
    }

    void "test something"() {
        setup:
        def exceptionThrown = false

        when:
        messagingService.registerReceiver(queue: 'hello', new ExpressionEvaluatingMessageReceiver('println'))
        messagingService.sendMessage(queue: 'hello', "world")
        messagingService.sendMessage(queue: 'hello', "goodbye")

        messagingService.registerReceiver(topic: 'hello', new ExpressionEvaluatingMessageReceiver('is'))
        messagingService.sendMessage(topic: 'hello', "world")

        //will log warning
        messagingService.sendMessage(queue: 'noreceiver', "goodbye")

        then:
        capture.toString().contains("WARN  ignite.IgniteMessagingQueueReceiverWrapper  - No receiver configured for queue noreceiver")
    }
}
