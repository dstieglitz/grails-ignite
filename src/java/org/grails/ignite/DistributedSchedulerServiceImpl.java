package org.grails.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of a simple distributed scheduled executor service that mimics the interface of the
 * ScheduledThreadPoolExector (at least some of the methods), but executes the actual jobs on the grid instead
 * of in a local ThreadPool.
 *
 * @author Dan Stieglitz
 */
public class DistributedSchedulerServiceImpl implements Service, SchedulerService {

    private static final Logger log = Logger.getLogger(DistributedSchedulerServiceImpl.class.getName());

    @IgniteInstanceResource
    private Ignite ignite;

    private IgniteSet<ScheduleData> schedule;

    private DistributedScheduledThreadPoolExecutor executor;

    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        ScheduleData scheduleData = new ScheduleData();
        scheduleData.setCommand(command);
        scheduleData.setInitialDelay(initialDelay);
        scheduleData.setPeriod(period);
        scheduleData.setTimeUnit(unit);
        schedule.add(scheduleData);
        log.info("added " + scheduleData + "to schedule");
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ScheduleData scheduleData = new ScheduleData();
        scheduleData.setCommand(command);
        scheduleData.setInitialDelay(initialDelay);
        scheduleData.setDelay(delay);
        scheduleData.setTimeUnit(unit);
        schedule.add(scheduleData);
        log.info("added " + scheduleData + "to schedule");
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void stopScheduler() {
        executor.setRunning(false);
    }

    @Override
    public void startScheduler() {
        executor.setRunning(true);
    }

    @Override
    public boolean isSchedulerRunning() {
        return executor.isRunning();
    }

    @Override
    public void cancel(ServiceContext serviceContext) {
        log.info("service " + this + "cancelled!");
    }

    @Override
    public void init(ServiceContext serviceContext) throws Exception {
        this.executor = new DistributedScheduledThreadPoolExecutor(ignite, 1);
        CollectionConfiguration config = new CollectionConfiguration();
        config.setCacheMode(CacheMode.REPLICATED);
        this.schedule = ignite.set("jobSchedules", config);
    }

    @Override
    public void execute(ServiceContext serviceContext) throws Exception {
        log.info("service "+this+" executed");
        log.debug("schedule.size()=" + this.schedule.size());
        for (ScheduleData datum : schedule) {
            log.debug("found existing schedule data " + datum);
            if (datum.getPeriod() > 0) {
                scheduleAtFixedRate(datum.command, datum.initialDelay, datum.period, datum.timeUnit);
            } else if (datum.getDelay() > 0) {
                scheduleWithFixedDelay(datum.command, datum.initialDelay, datum.delay, datum.timeUnit);
            }
        }
    }

    private class ScheduleData {
        private UUID uuid;
        private Runnable command;
        private long initialDelay = -1;
        private long period = -1;
        private long delay = -1;
        private TimeUnit timeUnit;

        public ScheduleData() {
            this.uuid = UUID.randomUUID();
        }

        private TimeUnit getTimeUnit() {
            return timeUnit;
        }

        private void setTimeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        private Runnable getCommand() {
            return command;
        }

        private void setCommand(Runnable command) {
            this.command = command;
        }

        private long getInitialDelay() {
            return initialDelay;
        }

        private void setInitialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
        }

        private long getPeriod() {
            return period;
        }

        private void setPeriod(long period) {
            this.period = period;
        }

        private long getDelay() {
            return delay;
        }

        private void setDelay(long delay) {
            this.delay = delay;
        }

        public String toString() {
            return uuid.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ScheduleData)) return false;
            return ((ScheduleData)obj).toString().equals(this.toString());
        }

        @Override
        public int hashCode() {
            return uuid.hashCode();
        }

    }
}
