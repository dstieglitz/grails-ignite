package grails.plugins.ignite

/**
 * Created by dstieglitz on 10/26/16.
 */
interface MessageReceiver {
    public void receive(Object destination, Object data);
}
