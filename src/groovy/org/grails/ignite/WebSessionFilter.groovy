package org.grails.ignite

import grails.util.Holders

import javax.servlet.FilterConfig
import javax.servlet.ServletException

/**
 * Created by dstieglitz on 9/1/15.
 */
class WebSessionFilter extends org.apache.ignite.cache.websession.WebSessionFilter {

    @Override
    void init(FilterConfig cfg) throws ServletException {
        // get grid name from application configuration
        OverridablePropertyFilterConfigDecorator decorator = new OverridablePropertyFilterConfigDecorator(cfg)
        def application = Holders.grailsApplication

        def configuredGridName = IgniteStartupHelper.DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        log.info "configuring web session clustering for gridName=${configuredGridName}"

        decorator.overrideInitParameter('IgniteWebSessionsGridName', configuredGridName)
        super.init(decorator)
    }
}
