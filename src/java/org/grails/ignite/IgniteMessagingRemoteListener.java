package org.grails.ignite;

import org.apache.ignite.lang.IgniteBiPredicate;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dstieglitz on 10/29/16.
 */
public class IgniteMessagingRemoteListener implements IgniteBiPredicate<UUID, Object>, Serializable {
    private Object destination;
    private MessageReceiver receiver;

    public IgniteMessagingRemoteListener(MessageReceiver receiver, Object destination) {
        this.destination = destination;
        this.receiver = receiver;
    }

    @Override
    public boolean apply(UUID nodeId, Object msg) {
        receiver.receive(destination, msg);
        return true;
    }
}
