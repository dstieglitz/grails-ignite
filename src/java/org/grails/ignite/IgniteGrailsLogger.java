package org.grails.ignite;

import org.apache.ignite.IgniteLogger;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dstieglitz on 9/24/15.
 */
public class IgniteGrailsLogger implements IgniteLogger {

    private static final Map<Object, IgniteLogger> igniteLoggerMap = new HashMap<Object, IgniteLogger>();
    private Logger underlyingLogger;

    public IgniteGrailsLogger() {

    }

    public IgniteGrailsLogger(Logger logger) {
        this.underlyingLogger = logger;
    }

    @Override
    public IgniteLogger getLogger(Object o) {
//        System.out.println("getLogger --> "+o);
        String className = null;

        if (o instanceof String) {
            className = (String) o;
        } else if (o instanceof Class) {
            className = ((Class) o).getName();
        } else {
            o = o.toString();
        }

        if (!igniteLoggerMap.containsKey(o)) {
            igniteLoggerMap.put(o, new IgniteGrailsLogger(Logger.getLogger(className)));
        }

        return igniteLoggerMap.get(o);
    }

    @Override
    public void trace(String s) {
        if (underlyingLogger == null) {
            System.out.println(s);
        } else {
            underlyingLogger.trace(s);
        }
    }

    @Override
    public void debug(String s) {
        if (underlyingLogger == null) {
            System.out.println(s);
        } else {
            underlyingLogger.debug(s);
        }
    }

    @Override
    public void info(String s) {
        if (underlyingLogger == null) {
            System.out.println(s);
        } else {
            underlyingLogger.info(s);
        }
    }

    @Override
    public void warning(String s) {
        if (underlyingLogger == null) {
            System.out.println(s);
        } else {
            underlyingLogger.warn(s);
        }
    }

    @Override
    public void warning(String s, Throwable throwable) {
        if (underlyingLogger == null) {
            System.out.println(s);
            throwable.printStackTrace();
        } else {
            underlyingLogger.warn(s, throwable);
        }
    }

    @Override
    public void error(String s) {
        if (underlyingLogger == null) {
            System.err.println(s);
        } else {
            underlyingLogger.error(s);
        }
    }

    @Override
    public void error(String s, Throwable throwable) {
        if (underlyingLogger == null) {
            System.err.println(s);
            throwable.printStackTrace();
        } else {
            underlyingLogger.error(s, throwable);
        }
    }

    @Override
    public boolean isTraceEnabled() {
        if (underlyingLogger == null) {
            return Logger.getRootLogger().isTraceEnabled();
        } else {
            return underlyingLogger.isTraceEnabled();
        }
    }

    @Override
    public boolean isDebugEnabled() {
        if (underlyingLogger == null) {
            return Logger.getRootLogger().isDebugEnabled();
        } else {
            return underlyingLogger.isDebugEnabled();
        }
    }

    @Override
    public boolean isInfoEnabled() {
        if (underlyingLogger == null) {
            return Logger.getRootLogger().isInfoEnabled();
        } else {
            return underlyingLogger.isInfoEnabled();
        }
    }

    @Override
    public boolean isQuiet() {
        return false;
    }

    @Override
    public String fileName() {
        return null;
    }
}
