package org.grails.ignite;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class manages instances of scheduled feeds in the Ignite grid. Instances of this class are managed in a schedule
 * set which is distributed across the grid so that if the singleton scheduler node goes down, whichever nodes takes
 * over can retrieve the current schedule from the grid and pick up the scheduling.
 */
public class ScheduledRunnable implements Callable, NamedRunnable, Serializable {
    private String name;
    private Runnable underlyingRunnable;
    private long initialDelay = -1;
    private long period = -1;
    private long delay = -1;
    private long timeout = 60000;
    private TimeUnit timeUnit;
    private String cronString;

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
        return this.getClass().getSimpleName() + "[name=\"" + name +
                "\", period=" + period +
                ", delay=" + delay +
                ", initialDelay=" + initialDelay +
                ", timeUnit=" + timeUnit +
                ", cronString=\"" + cronString + "\"]";
    }

    public String getName() {
        return name;
    }

    public void run() {
        underlyingRunnable.run();
    }

    public String getCronString() {
        return this.cronString;
    }

    public void setCronString(String cronString) {
        this.cronString = cronString;
    }

    public long getTimeout() {
        return this.timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ScheduledRunnable)) return false;
        return ((ScheduledRunnable) obj).getName().equals(this.getName());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public Object call() throws Exception {
        underlyingRunnable.run();
        return null;
    }
}
