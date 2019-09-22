package org.grails.ignite.examples;

import org.apache.ignite.Ignite;

/**
 * Abstract superclass for example Ignite classes
 *
 * @author Dan Stieglitz
 */
public abstract class AbstractIgniteApplication {
    protected Ignite ignite;

    public AbstractIgniteApplication(Ignite ignite) {
        this.ignite = ignite;
    }
}
