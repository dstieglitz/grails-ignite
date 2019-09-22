package org.grails.ignite;

/**
 * Created by dstieglitz on 4/1/16.
 */
public class DistributedRunnableException extends Exception {
    public DistributedRunnableException(String message) {
        super(message);
    }

    public DistributedRunnableException(String message, Throwable cause) {
        super(message, cause);
    }
}
