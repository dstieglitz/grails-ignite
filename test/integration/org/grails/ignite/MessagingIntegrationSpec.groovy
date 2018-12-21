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
        def testStrings = [
            "world test 123232",
            "goodbye test 5353535"
        ]

        when:
        messagingService.registerReceiver(queue: 'hello', new ExpressionEvaluatingMessageReceiver('println'))
        messagingService.sendMessage(queue: 'hello', testStrings[0])
        messagingService.sendMessage(queue: 'hello', testStrings[1])

        messagingService.registerReceiver(topic: 'hello', new ExpressionEvaluatingMessageReceiver('is'))
        messagingService.sendMessage(topic: 'hello', "world")

        //will log warning
        messagingService.sendMessage(queue: 'noreceiver', "goodbye")

        then:
        testStrings.each { capture.toString().contains(it) }
        capture.toString().contains("WARN  ignite.IgniteMessagingQueueReceiverWrapper  - No receiver configured for queue noreceiver")
    }
}
