package org.grails.ignite

import grails.plugins.*
import grails.util.Holders
import org.grails.ignite.IgniteContextBridge
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.core.Ordered

class IgniteGrailsPlugin extends Plugin {

    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "4.0.0 > *"
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
        "grails-app/views/error.gsp"
    ]

    // TODO Fill in these fields
    def title = "Grails Ignite Plugin" // Headline display name of the plugin
    def author = "Dan Stieglitz"
    def authorEmail = "dstieglitz@stainlesscode.com"
    def description = '''\
A plugin for the Apache Ignite data grid framework.
'''
    def profiles = ['plugin']

    // URL to the plugin's documentation
    def documentation = "https://github.com/dstieglitz/grails-ignite"

    // Extra (optional) plugin metadata

    // License: one of 'APACHE', 'GPL2', 'GPL3'
    def license = "APACHE"

    // Details of company behind the plugin (if there is one)
//    def organization = [ name: "My Company", url: "http://www.my-company.com/" ]

    // Any additional developers beyond the author specified above.
    def developers = [ [ name: "Ben Brown", email: "bbrown@stainlesscode.com" ]]

    // Location of the plugin's issue tracker.
    def issueManagement = [system: "GITHUB", url: "https://github.com/dstieglitz/grails-ignite/issues"]

    // Online location of the plugin's browseable source code.
    def scm = [url: "https://github.com/dstieglitz/grails-ignite"]

    Closure doWithSpring() { {->
        def configuredGridName = Holders.config.getProperty("ignite.gridName", String, IgniteStartupHelper.DEFAULT_GRID_NAME)

//        IgniteWebSessionsFilter(FilterRegistrationBean) {
//            filter = bean(WebSessionFilter)
//            initParameters = ['IgniteWebSessionsGridName':configuredGridName]
//            urlPatterns = ['/*']
//            order = Ordered.LOWEST_PRECEDENCE
//        }

        if (Holders.config.getProperty("ignite.enabled", Boolean, false)) {
            grid(IgniteContextBridge)
        }
        }
    }

    void doWithDynamicMethods() {
        // TODO Implement registering dynamic methods to classes (optional)
    }

    void doWithApplicationContext() {
        // TODO Implement post initialization spring config (optional)
    }

    void onChange(Map<String, Object> event) {
        // TODO Implement code that is executed when any artefact that this plugin is
        // watching is modified and reloaded. The event contains: event.source,
        // event.application, event.manager, event.ctx, and event.plugin.
    }

    void onConfigChange(Map<String, Object> event) {
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }

    void onShutdown(Map<String, Object> event) {
        // TODO Implement code that is executed when the application shuts down (optional)
    }
}
