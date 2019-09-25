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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * <p>An implementation of a simple distributed scheduler service that mimics the interface of the
 * ScheduledThreadPoolExector (at least some of the methods), but executes the actual jobs on the grid instead
 * of in a local ThreadPool.</p>
 * <p>Each job has a ScheduledRunnable record saved to the cluster in a REDUNDANT cluster-wide data structure. If the node
 * hosting this scheduler service goes down, another node can pick up the service and re-schedule the jobs for
 * execution.</p>
 * <p>The schedule uses a Set&lt;ScheduledRunnable&gt; object under the hood, so it's important to pay attention to naming
 * since two ScheduledRunnable objects with the same name are considered to be the same object (to prevent over-scheduling
 * of the same task).</p>
 *
 * @author Dan Stieglitz
 */
public class DistributedSchedulerServiceImpl implements Service, SchedulerService {

    private static final Logger log = Logger.getLogger(DistributedSchedulerServiceImpl.class.getName());
    private static final String JOB_SCHEDULE_DATA_SET_NAME = "jobSchedules";
    private static IgniteSet<ScheduledRunnable> schedule;
    @IgniteInstanceResource
    private Ignite ignite;
    private DistributedScheduledThreadPoolExecutor executor;
    // to allow cancellation
    private Map<String, ScheduledFuture<?>> nameFutureMap = new HashMap<String, ScheduledFuture<?>>();
    private long timeout = 60000;
    private int poolSize = 10;
    private TaskDecorator taskDecorator;

    public DistributedSchedulerServiceImpl() {
        // default constructor
    }

    public void setTaskDecorator(TaskDecorator taskDecorator) {
        this.taskDecorator = taskDecorator;
        if (executor != null) {
            executor.setTaskDecorator(taskDecorator);
        }
    }

    public DistributedSchedulerServiceImpl(int poolSize) {
        this.poolSize = poolSize;
    }

    public DistributedSchedulerServiceImpl(long timeout, int poolSize) {
        this.timeout = timeout;
        this.poolSize = poolSize;
    }

    public DistributedSchedulerServiceImpl(Ignite ignite) {
        this.ignite = ignite;
    }

    @Override
    public void init(ServiceContext serviceContext) throws Exception {
        log.debug("init [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        this.executor = new DistributedScheduledThreadPoolExecutor(this.ignite, this.poolSize);
        this.executor.setTaskDecorator(taskDecorator);
        schedule = initializeSet(ignite);

        log.info("scheduling cron jobs...");
        for (ScheduledRunnable datum : schedule) {
            if (datum.getCronString() != null) {
                try {
                    // if this block is called, we are re-scheduling some previously scheduled jobs.
                    // any scheduling errors that occur should already have occurred when the jobs
                    // were originally scheduled (and handled, presumably), so we just note them here but
                    // do not bubble up exceptions
                    log.info("found previously scheduled cron job " + datum);
                    ScheduledFuture future = executor.scheduleWithCron(datum, datum.getCronString());
                    nameFutureMap.put(datum.getName(), future);
                    log.debug("job scheduled and added to namedFutureMap");
                } catch (Throwable t) {
                    log.error(t.getMessage());
                }
            }
        }

        // FIXME set TaskDecorator?

        log.info("service " + this + " initialized");
    }

    private static IgniteSet initializeSet(Ignite ignite) throws IgniteException {
        log.info("initializing distributed dataset: " + JOB_SCHEDULE_DATA_SET_NAME);
        CollectionConfiguration setCfg = new CollectionConfiguration();
        setCfg.setAtomicityMode(TRANSACTIONAL);
        setCfg.setCacheMode(REPLICATED);
        IgniteSet<ScheduledRunnable> set = ignite.set(JOB_SCHEDULE_DATA_SET_NAME, setCfg);
        return set;
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(ScheduledRunnable scheduledRunnable) {
        log.debug("scheduleAtFixedRate [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("scheduleAtFixedRate '" + scheduledRunnable + "',"
                + scheduledRunnable.getInitialDelay() + ","
                + scheduledRunnable.getPeriod() + ","
                + scheduledRunnable.getTimeUnit());

        ScheduledFuture future = executor.scheduleAtFixedRate(scheduledRunnable,
                scheduledRunnable.getInitialDelay(),
                scheduledRunnable.getPeriod(),
                scheduledRunnable.getTimeUnit());

        log.debug("schedule returned " + future);
        log.debug("scheduling with timeout=" + timeout);

        ignite.compute().withName(scheduledRunnable.getName()).withTimeout(timeout).run(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduledRunnable));

        log.info("added " + scheduledRunnable + " to schedule");
        log.debug("scheduledRunnable: " + schedule);

        log.debug("added " + scheduledRunnable.getName() + ", " + future + " to namedFutureMap");
        nameFutureMap.put(scheduledRunnable.getName(), future);

        return future;
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(ScheduledRunnable scheduledRunnable) {
        log.debug("scheduleWithFixedDelay [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("scheduleWithFixedDelay '" + scheduledRunnable + "',"
                + scheduledRunnable.getInitialDelay() + ","
                + scheduledRunnable.getDelay() + ","
                + scheduledRunnable.getTimeUnit());

        ScheduledFuture future = executor.scheduleWithFixedDelay(scheduledRunnable,
                scheduledRunnable.getInitialDelay(),
                scheduledRunnable.getDelay(),
                scheduledRunnable.getTimeUnit());

        log.debug("schedule returned " + future);
        log.debug("scheduling with timeout=" + timeout);

        ignite.compute().withName(scheduledRunnable.getName()).withTimeout(timeout).run(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduledRunnable));

        log.info("added " + scheduledRunnable + " to schedule");
        log.debug("scheduledRunnable: " + schedule);

        log.debug("added " + scheduledRunnable.getName() + ", " + future + " to namedFutureMap");
        nameFutureMap.put(scheduledRunnable.getName(), future);

        return future;
    }

    @Override
    public ScheduledFuture scheduleWithCron(ScheduledRunnable scheduledRunnable) throws DistributedRunnableException {
        log.debug("scheduleWithCron [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        if (scheduledRunnable.getCronString() == null) {
            throw new DistributedRunnableException("No cron string provided for requested cron schedule: " + scheduledRunnable);
        }

        try {
            log.debug("scheduleWithCron '" + scheduledRunnable + "'," + scheduledRunnable.getCronString());

            // SERVICE FAILURE HAPPENS HERE
            ScheduledFuture future = executor.scheduleWithCron(scheduledRunnable, scheduledRunnable.getCronString());

            log.debug("schedule returned " + future);
            log.debug("scheduling with timeout=" + timeout);

            ignite.compute().withName(scheduledRunnable.getName()).withTimeout(timeout).run(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduledRunnable));
            log.info("added " + scheduledRunnable + " to schedule");
            log.debug("scheduledRunnable: " + schedule);

            log.debug("added " + scheduledRunnable.getName() + ", " + future + " to namedFutureMap");
            nameFutureMap.put(scheduledRunnable.getName(), future);

            return future;
        } catch (Throwable t) {
            throw new DistributedRunnableException(t.getMessage(), t);
        }
    }

    @Override
    public ScheduledFuture schedule(ScheduledRunnable scheduledRunnable) {
        log.debug("schedule [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("schedule '" + scheduledRunnable + "'," + scheduledRunnable.getDelay() + "," + scheduledRunnable.getTimeUnit());

        ScheduledFuture future = executor.schedule((Runnable) scheduledRunnable,
                scheduledRunnable.getDelay(),
                scheduledRunnable.getTimeUnit());

        log.debug("schedule returned " + future);

        ignite.compute().withName(scheduledRunnable.getName()).withTimeout(timeout).run(new SetClosure(ignite.name(), JOB_SCHEDULE_DATA_SET_NAME, scheduledRunnable));
        log.info("added " + scheduledRunnable + " to schedule");
        log.debug("scheduledRunnable: " + schedule);

        log.debug("added " + scheduledRunnable.getName() + ", " + future + " to namedFutureMap");
        nameFutureMap.put(scheduledRunnable.getName(), future);

        return future;
    }

    @Override
    public Future executeNow(ScheduledRunnable scheduledRunnable) {
        log.debug("schedule [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("schedule '" + scheduledRunnable + "'," + scheduledRunnable.getDelay() + "," + scheduledRunnable.getTimeUnit());

        Future future = executor.submit((Callable) scheduledRunnable);

        log.debug("submit returned " + future);

        return future;
    }

    @Override
    public Future executeNowOnThisNode(ScheduledRunnable scheduledRunnable) {
        log.debug("schedule [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("schedule '" + scheduledRunnable + "'," + scheduledRunnable.getDelay() + "," + scheduledRunnable.getTimeUnit());

        Future future = executor.submitToThisNode((Callable) scheduledRunnable);

        log.debug("submit returned " + future);

        return future;
    }

    /**
     * Query the state of the scheduled jobs to determine if a job with the supplied ID is scheduled.
     *
     * @param id
     * @return true if a ScheduledRunnable record exists for the job
     */
    @Override
    public boolean isScheduled(String id) {
        log.debug("isScheduled [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        for (ScheduledRunnable scheduleDatum : schedule) {
            if (scheduleDatum.getName().equals(id)) return true;
        }

        return false;
    }

    private ScheduledRunnable findScheduleDataByName(String name) {
        for (ScheduledRunnable scheduleDatum : schedule) {
            if (scheduleDatum.getName().equals(name)) return scheduleDatum;
        }

        return null;
    }

    // not in service interface
    public Map<String, ScheduledFuture<?>> getNameFutureMap() {
        return this.nameFutureMap;
    }

    @Override
    public boolean cancel(String name, boolean mayInterruptIfRunning) {
        log.debug("cancel [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.info("cancel '" + name + "', " + mayInterruptIfRunning);
        Future future = nameFutureMap.get(name);
        log.debug("found future " + future);

        if (future == null) {
            if (!isScheduled(name)) {
                log.debug("job is not scheduled, returning...");
                return true;
            }
            log.warn("tried to cancel, but no Future found for '" + name + "'");
            ScheduledRunnable sr = findScheduleDataByName(name);
            log.debug("found ScheduledRunnable " + sr + " in schedule");
            log.trace("before remove schedule size is " + schedule.size());
            boolean removed = true;
            schedule.remove(sr); // can't rely on the return value
            if (schedule.contains(sr)) {
                log.warn("could not cancel: job not removed from schedule");
                removed = false;
            }
            log.trace("after remove schedule size is " + schedule.size());
            log.debug("remove from schedule " + name + " returned " + removed);
            return removed;
        } else {
//            Future future = data.getFuture();
            log.debug("cancelling via Future " + future);

            // getFuture() will return a ScheduledFutureTask
            boolean cancelled = executor.cancel((Runnable) future, true);

            log.debug("cancel returned " + cancelled);
            boolean removed = false;
            if (cancelled) {
                nameFutureMap.remove(name);
                if (!isScheduled(name)) {
                    log.debug("job is not scheduled, returning...");
                    return true;
                }
                ScheduledRunnable sr = findScheduleDataByName(name);
                log.debug("found ScheduledRunnable " + sr + " in schedule");
                log.trace("before remove schedule size is " + schedule.size());
                schedule.remove(sr); // can't rely on the return value
                if (schedule.contains(sr)) {
                    log.warn("could not cancel: job not removed from schedule");
                    removed = false;
                } else {
                    removed = true;
                }
                log.trace("after remove schedule size is " + schedule.size());
                log.debug("remove from schedule " + name + " returned " + removed);
            }

            return cancelled && removed;
        }
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

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getPoolSize() {
        return this.poolSize;
    }

    @Override
    public void execute(ServiceContext serviceContext) throws Exception {
        log.debug("execute [thread=\"" + Thread.currentThread().getName() + "\", hash=\"" + System.identityHashCode(this) + "\"]");
        log.debug("schedule.size()=" + this.schedule.size());
        for (ScheduledRunnable datum : schedule) {
            log.debug("found existing schedule data " + datum);
            if (datum.getPeriod() > 0) {
                scheduleAtFixedRate(datum);
            } else if (datum.getDelay() > 0) {
                scheduleWithFixedDelay(datum);
            }
        }
        log.debug("exiting service " + this + " execute");
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
        private final ScheduledRunnable scheduledRunnable;
        private final String gridName;

        /**
         * @param setName Set name.
         * @param data    The data to add.
         */
        SetClosure(String gridName, String setName, ScheduledRunnable data) {
            this.setName = setName;
            this.scheduledRunnable = data;
            this.gridName = gridName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            IgniteSet<ScheduledRunnable> set = Ignition.ignite(gridName).set(setName, null);
            set.add(scheduledRunnable);
        }
    }
}
