package org.grails.ignite

import javax.servlet.FilterConfig
import javax.servlet.ServletContext

/**
 * A simple decorator over a FilterConfig object that permits overriding of init parameters so that we can pull
 * values from the Grails Config
 * Created by dstieglitz on 9/1/15.
 */
class OverridablePropertyFilterConfigDecorator implements FilterConfig {

    private FilterConfig underlyingFilterConfig
    private Map overridedInitParameters = [:]

    public OverridablePropertyFilterConfigDecorator(FilterConfig config) {
        this.underlyingFilterConfig = config;
    }

    @Override
    String getFilterName() {
        return underlyingFilterConfig.getFilterName();
    }

    @Override
    ServletContext getServletContext() {
        return underlyingFilterConfig.getServletContext();
    }

    @Override
    String getInitParameter(String name) {
        if (overridedInitParameters.containsKey(name)) {
            return overridedInitParameters[name]
        }
        else {
            return underlyingFilterConfig.getInitParameter(name)
        }
    }

    public void overrideInitParameter(String name, String value) {
        overridedInitParameters[name] = value
    }

    @Override
    Enumeration<String> getInitParameterNames() {
        return underlyingFilterConfig.getInitParameterNames()
    }
}
