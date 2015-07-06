package gilmour;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import static gilmour.GilmourProtocol.makeSenderId;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface Gilmour {
    class Subscribers extends HashMap<String, ArrayList<GilmourSubscription>> {}
    GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts);
    void unsubscribe(String topic, GilmourSubscription h);
    void unsubscribe(String topic);
    public String responseTopic(String sender);
    <T> void publish(String topic, T data, int code, String sender);
    boolean isAResponse(String topic);
    boolean canReportErrors();
    void reportError(GilmourProtocol.GilmourErrorResponse message);
    boolean acquire_group_lock(String group, String sender);
    void start();

    default <T> String publish(String topic, T data) {
        final String sender = makeSenderId();
        this.publish(topic, data, 200, sender);
        return sender;
    }

    default <T> String publish(String topic, T data, int code) {
        final String sender = makeSenderId();
        this.publish(topic, data, code, sender);
        return sender;
    }

    default <T> String publish(String topic, T data, GilmourHandler respHandler) {
        final String sender = makeSenderId();
        final String respChannel = responseTopic(sender);
        final GilmourHandlerOpts opts = GilmourHandlerOpts.createGilmourHandlerOpts().setOneshot().setSendResponse(false);
        subscribe(respChannel, respHandler, opts);
        publish(topic, data, 200, sender);
        return sender;
    }

    default void execSubscribers(Subscribers subscribers, String key, String topic, String message) {
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

    default void executeSubscriber(GilmourSubscription sub, String topic, String data) {
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

    default void handleRequest(GilmourSubscription sub, String topic, GilmourProtocol.RecvGilmourData d) {
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

    default void addSubscriber(Subscribers subscribers, String topic, GilmourSubscription sub) {
        synchronized (subscribers) {
            if (subscribers.get(topic) == null) {
                subscribers.put(topic, new ArrayList<>());
            }
            subscribers.get(topic).add(sub);
        }
    }

    default ArrayList<GilmourSubscription> removeSubscriber(Subscribers subscribers, String topic, GilmourSubscription sub) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(topic);
            subs.remove(sub);
            return subs;
        }
    }
    default void clearSubscribers(Subscribers subscribers, String topic) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(topic);
            subs.clear();
        }
    }
}
