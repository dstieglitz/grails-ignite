package org.grails.ignite;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class manages instances of scheduled feeds in the Ignite grid. Instances of this class are managed in a schedule
 * set which is distributed across the grid so that if the singleton scheduler node goes down, whichever nodes takes
 * over can retrieve the current schedule from the grid and pick up the scheduling.
 */
public class ScheduledRunnable implements NamedRunnable, Serializable {
    private String name;
    private Runnable underlyingRunnable;
    private long initialDelay = -1;
    private long period = -1;
    private long delay = -1;
    private TimeUnit timeUnit;
//    private ScheduledFuture future;

    public ScheduledRunnable() {
        this.name = UUID.randomUUID().toString();
    }

    public ScheduledRunnable(String name) {
        this.name = name;
    }

    public ScheduledRunnable(Runnable runnable) {
        if (runnable instanceof NamedRunnable) {
            this.name = ((NamedRunnable) runnable).getName();
        }
        this.underlyingRunnable = runnable;
        this.name = UUID.randomUUID().toString();
    }

    public ScheduledRunnable(String name, Runnable runnable) {
        if (runnable instanceof NamedRunnable) {
            this.name = ((NamedRunnable) runnable).getName();
        }
        this.underlyingRunnable = runnable;
        this.name = name;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Runnable getUnderlyingRunnable() {
        return underlyingRunnable;
    }
//
//    public void setRunnable(Runnable underlyingRunnable) {
//        this.underlyingRunnable = underlyingRunnable;
//    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public void run() {
        underlyingRunnable.run();
    }

//    public ScheduledFuture getFuture() {
//        return this.future;
//    }
//
//    public void setScheduledFuture(ScheduledFuture future) {
//        this.future = future;
//    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ScheduledRunnable)) return false;
        return ((ScheduledRunnable) obj).toString().equals(this.toString());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

}
