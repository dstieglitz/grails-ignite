package grails.plugins.ignite

import grails.test.mixin.integration.Integration
import spock.lang.Specification
import java.util.concurrent.TimeUnit
import resources.MockRunnable

@Integration
class DistributedSchedulerIntegrationSpec extends Specification {

    DistributedSchedulerService distributedSchedulerService

    def setup() {
    }

    def cleanup() {
    }

    void "schedules at a fixed rate"() {
        setup:
        def runnable = new MockRunnable()
        distributedSchedulerService.startScheduler()

        when:
        distributedSchedulerService.scheduleAtFixedRate(runnable, 0, 100, TimeUnit.MILLISECONDS, 0)
        sleep(1000)

        then:
        runnable.callCount >= 2
        def avg = runnable.delayTimes[1..-1].with {
            sum() / size()
        }
        avg >= 50 && avg <= 150
    }

    void "schedules with a fixed delay"() {
        setup:
        def runnable = new MockRunnable()
        distributedSchedulerService.startScheduler()

        when:
        distributedSchedulerService.scheduleWithFixedDelay(runnable, 0, 50, TimeUnit.MILLISECONDS, 0)
        sleep(120)

        then:
        runnable.callCount >= 2
        runnable.delayTimes[1..-1].each { assert it >= 50 }
    }

    void "schedules a single time"() {
        setup:
        def runnable = new MockRunnable()
        distributedSchedulerService.startScheduler()

        when:
        distributedSchedulerService.schedule(runnable, 0, TimeUnit.MILLISECONDS, 0)
        sleep(120)

        then:
        runnable.callCount == 1
    }

    void "schedules using a cron expression"() {
        setup:
        def runnable = new MockRunnable()
        distributedSchedulerService.startScheduler()
        def cronExp = "* * * * *"

        when:
        IgniteCronDistributedRunnableScheduledFuture future = (IgniteCronDistributedRunnableScheduledFuture) distributedSchedulerService.scheduleWithCron(
            runnable, cronExp, 0, "name"
        )

        then:
        future.cronTaskId
        future.toDataMap().cronExpression == cronExp
    }

    void "schedules an anonymous class"() {
        setup:
        def runnable = new Runnable () {
            public boolean called = false

            public void run () {
                called = true
            }
        }
        distributedSchedulerService.startScheduler()

        when:
        distributedSchedulerService.schedule(runnable, 0, TimeUnit.MILLISECONDS, 0)
        sleep(120)

        then:
        runnable.called
    }

    //because Groovy 2.x doesn't support lambdas (use Groovy 3)
    void "schedules a closure"() {
        setup:
        def called = false
        Runnable runnable = { called = true } as Runnable
        distributedSchedulerService.startScheduler()

        when:
        distributedSchedulerService.schedule(runnable, 0, TimeUnit.MILLISECONDS, 0)
        sleep(120)

        then:
        called
    }
}
