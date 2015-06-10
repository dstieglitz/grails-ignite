package org.grails.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * <p>An implementation of a simple distributed scheduled executor service that mimics the interface of the
 * ScheduledThreadPoolExector (at least some of the methods), but executes the actual jobs on the grid instead
 * of in a local ThreadPool.</p>
 * <p>Each job has a ScheduleData record saved to the cluster in a REDUNDANT cluster-wide data structure. If the node
 * hosting this scheduler service goes down, another node can pick up the service and re-schedule the jobs for
 * execution.</p>
 * <p>The schedule uses a Set<ScheduleData> object under the hood, so it's important to pay attention to naming
 * since two ScheduleData objects with the same name are considered to be the same object (to prevent over-scheduling
 * of the same task).</p>
 *
 * @author Dan Stieglitz
 */
public class DistributedSchedulerServiceImpl implements Service, SchedulerService {

    private static final Logger log = Logger.getLogger(DistributedSchedulerServiceImpl.class.getName());
    private static final String JOB_SCHEDULE_DATA_SET_NAME = "jobSchedules";
    private static IgniteSet<ScheduleData> schedule;
    @IgniteInstanceResource
    private Ignite ignite;
    private DistributedScheduledThreadPoolExecutor executor;

    public DistributedSchedulerServiceImpl() {
        this.ignite = ignite;
    }

    public DistributedSchedulerServiceImpl(Ignite ignite) {
        this.ignite = ignite;
    }

    private static IgniteSet initializeSet(Ignite ignite) throws IgniteException {
        log.info("initializing set: " + JOB_SCHEDULE_DATA_SET_NAME);
        CollectionConfiguration setCfg = new CollectionConfiguration();
        setCfg.setAtomicityMode(TRANSACTIONAL);
        setCfg.setCacheMode(PARTITIONED);
        IgniteSet<ScheduleData> set = ignite.set(JOB_SCHEDULE_DATA_SET_NAME, setCfg);
        return set;
    }

    public ScheduledFuture scheduleAtFixedRate(NamedRunnable command, long initialDelay, long period, TimeUnit unit) {
        // FIXME concurrent commands will be removed by set semantics, need to re-name with unique ID here
        ScheduleData scheduleData = new ScheduleData(command.getName());
        scheduleData.setCommand(command);
        scheduleData.setInitialDelay(initialDelay);
        scheduleData.setPeriod(period);
        scheduleData.setTimeUnit(unit);
        ignite.compute().broadcast(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduleData));
        log.info("added " + scheduleData + " to schedule");
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture scheduleWithFixedDelay(NamedRunnable command, long initialDelay, long delay, TimeUnit unit) {
        // FIXME concurrent commands will be removed by set semantics, need to re-name with unique ID here
        ScheduleData scheduleData = new ScheduleData(command.getName());
        scheduleData.setCommand(command);
        scheduleData.setInitialDelay(initialDelay);
        scheduleData.setDelay(delay);
        scheduleData.setTimeUnit(unit);
        ignite.compute().broadcast(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduleData));
        log.info("added " + scheduleData + " to schedule");
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * Query the state of the scheduled jobs to determine if a job with the supplied ID is scheduled.
     *
     * @param id
     * @return true if a ScheduleData record exists for the job
     */
    @Override
    public boolean isScheduled(String id) {
        for (ScheduleData scheduleDatum : schedule) {
            if (scheduleDatum.toString().equals(id)) return true;
        }

        return false;
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
        schedule = initializeSet(ignite);
    }

    @Override
    public void execute(ServiceContext serviceContext) throws Exception {
        log.info("service " + this + " executed");
        log.debug("schedule.size()=" + this.schedule.size());
        for (ScheduleData datum : schedule) {
            log.debug("found existing schedule data " + datum);
            if (datum.getPeriod() > 0) {
                scheduleAtFixedRate(datum, datum.initialDelay, datum.period, datum.timeUnit);
            } else if (datum.getDelay() > 0) {
                scheduleWithFixedDelay(datum, datum.initialDelay, datum.delay, datum.timeUnit);
            }
        }
    }

    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Closure to populate the set.
     */
    private static class SetClosure implements IgniteRunnable {
        /**
         * Set name.
         */
        private final String setName;
        private final ScheduleData scheduleData;
        private final String gridName;

        /**
         * @param setName Set name.
         * @param data    The data to add.
         */
        SetClosure(String gridName, String setName, ScheduleData data) {
            this.setName = setName;
            this.scheduleData = data;
            this.gridName = gridName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            IgniteSet<ScheduleData> set = Ignition.ignite(gridName).set(setName, null);
            set.add(scheduleData);
        }
    }

    private class ScheduleData implements NamedRunnable, Serializable {
        private String id;
        private Runnable command;
        private long initialDelay = -1;
        private long period = -1;
        private long delay = -1;
        private TimeUnit timeUnit;

        public ScheduleData() {
            this.id = UUID.randomUUID().toString();
        }

        public ScheduleData(String id) {
            this.id = id;
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
            return id;
        }

        public String getName() {
            return id;
        }

        public void run() {
            command.run();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ScheduleData)) return false;
            return ((ScheduleData) obj).toString().equals(this.toString());
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

    }
}
