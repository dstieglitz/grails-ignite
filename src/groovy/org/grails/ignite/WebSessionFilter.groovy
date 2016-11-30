package org.grails.ignite

import grails.util.Holders
import groovy.util.logging.Log4j
import org.codehaus.groovy.grails.web.util.WebUtils
import org.springframework.util.AntPathMatcher

import javax.servlet.*
import javax.servlet.http.HttpServletRequest

/**
 * Created by dstieglitz on 9/1/15.
 */
@Log4j
class WebSessionFilter extends org.apache.ignite.cache.websession.WebSessionFilter {

    def excludes

    @Override
    void init(FilterConfig cfg) throws ServletException {
        // get grid name from application configuration
        OverridablePropertyFilterConfigDecorator decorator = new OverridablePropertyFilterConfigDecorator(cfg)
        def application = Holders.grailsApplication

        def configuredGridName = IgniteStartupHelper.DEFAULT_GRID_NAME
        if (!(application.config.ignite.gridName instanceof ConfigObject)) {
            configuredGridName = application.config.ignite.gridName
        }

        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        decorator.overrideInitParameter('IgniteWebSessionsGridName', configuredGridName)

        this.excludes = application.config.ignite.webSessionClusteringPathExcludes
        if (this.excludes instanceof ConfigObject) {
            this.excludes = []
        }

        if (webSessionClusteringEnabled) {
            log.info "excluding the following URIs from web session clustering by configuration: ${this.excludes}"

            if (IgniteStartupHelper.grid == null) {
                log.info "web session clustering is enabled but grid is not started, starting now"
                IgniteStartupHelper.startIgnite();
            }
            log.info "configuring web session clustering for gridName=${configuredGridName}"
            super.init(decorator)
        }
    }

    private boolean shouldExclude(String path) {
        log.trace "shouldExclude '${path}' ?"
        AntPathMatcher pathMatcher = new AntPathMatcher();

        for (String exclude : excludes) {
            log.trace "checking ${exclude}"

            // FIXME use a regex
            if (pathMatcher.match(exclude, path)) {
                log.trace "found excluded prefix ${path}"
                return true
            }
        }

        return false
    }

    @Override
    void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpReq = (HttpServletRequest) req;
        String path = httpReq.getServletPath();
        log.debug "doFilter ${path}"

        try {
            def flashScope = WebUtils.retrieveGrailsWebRequest().flashScope
            log.trace "flashScope=${flashScope} (type=${flashScope?.class?.name})"
            flashScope.keySet().each {
                log.debug "flash.${it}=${flashScope.get(it)}"
            }
        } catch (java.lang.IllegalStateException e) {
            log.debug e.message
        }

        // if Shiro sessions disabled, this will throw an error
//        if (log.traceEnabled) {
//            httpReq.session.attributeNames.each {
//                log.trace "request attribute ${it}=${httpReq.session.getAttribute(it)}"
//            }
//        }

        def application = Holders.grailsApplication
        def webSessionClusteringEnabled = (!(application.config.ignite.webSessionClusteringEnabled instanceof ConfigObject)
                && application.config.ignite.webSessionClusteringEnabled.equals(true))

        log.trace "webSessionClusteringEnabled=${webSessionClusteringEnabled}"

        if (webSessionClusteringEnabled && !shouldExclude(path)) {
            log.debug "invoking Ignite WebSessionFilter"
            super.doFilter(req, res, chain)
        } else {
            log.trace "chain.doFilter..."
            chain.doFilter(req, res)
        }
    }
}
