package org.grails.ignite;

/**
 * Implementations of this interface introduce a name to identify Runnables for a scheduling system.
 */
public interface NamedRunnable extends Runnable {

    public String getName();

}
