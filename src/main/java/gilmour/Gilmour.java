package gilmour;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import static gilmour.GilmourProtocol.makeSenderId;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public abstract class Gilmour {
    private class Subscribers extends HashMap<String, ArrayList<GilmourSubscription>> {}
    private Subscribers subscribers = new Subscribers();

    public abstract GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts);
    public abstract void unsubscribe(String topic, GilmourSubscription s);
    public abstract void unsubscribe(String topic);

    public abstract String responseTopic(String sender);
    public abstract <T> void publish(String topic, T data, int code, String sender);
    public abstract boolean isAResponse(String topic);
    public abstract boolean canReportErrors();
    public abstract void reportError(GilmourProtocol.GilmourErrorResponse message);
    public abstract boolean acquire_group_lock(String group, String sender);
    public abstract void start();

    public <T> String publish(String topic, T data) {
        final String sender = makeSenderId();
        this.publish(topic, data, 200, sender);
        return sender;
    }

    public <T> String publish(String topic, T data, int code) {
        final String sender = makeSenderId();
        this.publish(topic, data, code, sender);
        return sender;
    }

    public <T> String publish(String topic, T data, GilmourHandler respHandler) {
        final String sender = makeSenderId();
        final String respChannel = responseTopic(sender);
        final GilmourHandlerOpts opts = GilmourHandlerOpts.createGilmourHandlerOpts().setOneshot().setSendResponse(false);
        subscribe(respChannel, respHandler, opts);
        publish(topic, data, 200, sender);
        return sender;
    }

    protected void execSubscribers(String key, String topic, String message) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(key);
            for(Iterator<GilmourSubscription> si = subs.iterator(); si.hasNext();) {
                GilmourSubscription s = si.next();
                if (s.getOpts().isOneshot())
                    si.remove();
                this.executeSubscriber(s, topic, message);
            }
        }
    }

    protected void executeSubscriber(GilmourSubscription sub, String topic, String data) {
        final GilmourProtocol.RecvGilmourData d = GilmourProtocol.parseJson(data);
        GilmourHandlerOpts opts = sub.getOpts();
        if (opts.getGroup() != null) {
            if (!acquire_group_lock(opts.getGroup(), d.getSender()))
                return;
        }
        new Thread(() -> {
            this.handleRequest(sub, topic, d);
        }).start();
    }

    private void handleRequest(GilmourSubscription sub, String topic, GilmourProtocol.RecvGilmourData d) {
        GilmourRequest req = new RedisGilmourRequest(topic, d);
        GilmourResponder res = new RedisGilmourResponder(d.getSender());
        try {
            sub.getHandler().process(req, res);
        }
        catch (Exception e) {
            final GilmourProtocol.GilmourErrorResponse error = new GilmourProtocol.GilmourErrorResponse(500, d.getSender(),
                    topic, req.stringData(), e.getMessage(),
                    Arrays.toString(e.getStackTrace()));
            if (sub.getOpts().sendResponse())
                res.respond(error, 500);
            if (canReportErrors()) {
                reportError(error);
            }
        }
        finally {
            if (sub.getOpts().sendResponse())
                res.send(this);
        }
    }

    protected GilmourSubscription add_subscriber(String topic, GilmourHandler h, GilmourHandlerOpts opts) {
        final GilmourSubscription sub = new GilmourSubscription(h, opts);
        synchronized (subscribers) {
            if (subscribers.get(topic) == null) {
                subscribers.put(topic, new ArrayList<>());
            }
            subscribers.get(topic).add(sub);
        }
        return sub;
    }

    protected ArrayList<GilmourSubscription> remove_subscriber(String topic, GilmourSubscription sub) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(topic);
            subs.remove(sub);
            return subs;
        }
    }

    protected void remove_subscribers(String topic) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(topic);
            subs.clear();
        }
    }
}
