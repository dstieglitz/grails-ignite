package org.grails.ignite.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: dstieglitz
 * Date: 6/6/15
 * Time: 2:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class FirstIgniteComputeApplication extends AbstractIgniteApplication implements Runnable {
    public FirstIgniteComputeApplication(Ignite ignite) {
        super(ignite);
    }

    public void run() {
        Collection<IgniteCallable<Integer>> calls = new ArrayList();

        // Iterate through all the words in the sentence and create Callable jobs.
        for (final String word : "Count characters using callable".split(" ")) {
            calls.add(new IgniteCallable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return word.length();
                }
            });
        }

        // Execute collection of Callables on the grid.
        Collection<Integer> res = ignite.compute().call(calls);

        int sum = 0;

        // Add up individual word lengths received from remote nodes.
        for (int len : res)
            sum += len;

        System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
    }
}
