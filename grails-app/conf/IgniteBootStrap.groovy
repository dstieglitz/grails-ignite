import org.grails.ignite.DistributedSchedulerServiceImpl
import org.grails.ignite.HelloWorldGroovyTask

import java.util.concurrent.TimeUnit

class IgniteBootStrap {
    def grailsApplication
//    def distributedSchedulerService

    def init = { servletContext ->
        def ignite = grailsApplication.mainContext.getBean('grid')
        ignite.services().deployClusterSingleton("distributedSchedulerService", new DistributedSchedulerServiceImpl());

        // run the examples to test configuration
//        new FirstIgniteComputeApplication(ignite).run();
//        new FirstIgniteDataGridApplication(ignite).run();
//        new ExecutorServiceApplication(ignite).run();

        // example: schedule a task at fixed rate
       //distributedSchedulerService.scheduleAtFixedRate(new HelloWorldGroovyTask(), 0, 1000, TimeUnit.MILLISECONDS);
    }

    def destroy = {
        // destroy app
    }
}