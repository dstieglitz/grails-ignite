package org.grails.ignite

/**
 * <b>Work-in-progress</b>
 * <p>A wrapper class designed to allow the submission of a closure to the Ignite task grid</p>
 */
class IgniteClosureJobWrapper implements Runnable, Serializable {

    private Closure closure;
    private Object result;

    public IgniteClosureJobWrapper(Closure closure) {
        this.closure = closure;
    }

    public void run() {
        this.result = closure.call();
    }

    public Object get() {
        return this.result;
    }
}
