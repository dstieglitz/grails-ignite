import org.apache.ignite.Ignite

class IgniteBootStrap {
    def grailsApplication

    def init = { servletContext ->
        def grid = grailsApplication.mainContext.getBean('grid')
    }
    def destroy = {
        // destroy app
    }
}