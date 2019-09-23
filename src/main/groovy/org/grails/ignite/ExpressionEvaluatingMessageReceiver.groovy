package org.grails.ignite

import groovy.util.logging.Log4j

/**
 * Created by dstieglitz on 10/26/16.
 */
/**
 * A simple receiver that uses Groovy to evaluate the string expression supplied in the constructor as code. Useful
 * for adapting MDBs that call methods to using the Ignite MessagingService provided by the plugin
 */
@Log4j
class ExpressionEvaluatingMessageReceiver implements MessageReceiver {
    private String expression

    public ExpressionEvaluatingMessageReceiver(String expression) {
        this.expression = expression;
    }

    @Override
    public void receive(Object destination, Object data) {
        log.debug "receive(${data})"
        "${this.expression}"(data);
    }
}
