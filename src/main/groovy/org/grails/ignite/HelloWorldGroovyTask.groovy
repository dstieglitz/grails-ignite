package org.grails.ignite

/**
 * An example Runnable used to demonstrate the DistributedSchedulerService
 */
public class HelloWorldGroovyTask implements Serializable, Runnable {
    @Override
    public void run() {
        System.out.println("hello from " + this.getClass().getName() + " at " + new Date());
    }
}
