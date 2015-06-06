import org.apache.ignite.lang.IgniteCallable
import org.grails.ignite.examples.FirstIgniteComputeApplication
import org.grails.ignite.examples.FirstIgniteDataGridApplication

class IgniteBootStrap {
    def grailsApplication

    def init = { servletContext ->
        def ignite = grailsApplication.mainContext.getBean('grid')

        // run the examples to test configuration
        new FirstIgniteComputeApplication(ignite).run();
        new FirstIgniteDataGridApplication(ignite).run();
    }

    def destroy = {
        // destroy app
    }
}