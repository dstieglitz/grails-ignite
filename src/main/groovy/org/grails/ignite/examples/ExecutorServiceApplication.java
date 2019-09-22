package org.grails.ignite.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.concurrent.ExecutorService;

/**
 * Example grid executor class taken from the Ignite wiki.
 *
 * @author Dan Stieglitz
 */
public class ExecutorServiceApplication extends AbstractIgniteApplication implements Runnable {
    public ExecutorServiceApplication(Ignite ignite) {
        super(ignite);
    }

    public void run() {
        // Get cluster-enabled executor service.
        ExecutorService exec = ignite.executorService();

        // Iterate through all words in the sentence and create jobs.
        for (final String word : "Print words using runnable".split(" ")) {
            // Execute runnable on some node.
            exec.submit(new IgniteRunnable() {
                @Override
                public void run() {
                    System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                }
            });
        }
    }

}
