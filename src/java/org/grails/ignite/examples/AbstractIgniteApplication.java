package org.grails.ignite.examples;

import org.apache.ignite.Ignite;

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 6/6/15
 * Time: 2:47 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractIgniteApplication {
    protected Ignite ignite;

    public AbstractIgniteApplication(Ignite ignite) {
        this.ignite = ignite;
    }
}
