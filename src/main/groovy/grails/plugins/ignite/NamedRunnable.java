package grails.plugins.ignite;

import org.apache.ignite.lang.IgniteRunnable;

/**
 * Implementations of this interface introduce a name to identify Runnables for a scheduling system.
 */
public interface NamedRunnable extends IgniteRunnable {

    public String getName();

}
