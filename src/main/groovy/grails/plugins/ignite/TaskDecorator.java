package grails.plugins.ignite;

import java.util.concurrent.RunnableScheduledFuture;

/**
 * Delegate interface to allow
 */
public class TaskDecorator {
    protected <V> RunnableScheduledFuture<V> decorateTask(
            Runnable runnable, RunnableScheduledFuture<V> task) {
        return task;
    }
}
