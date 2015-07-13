package gilmour;

import gilmour.protocol.GilmourProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static gilmour.protocol.GilmourProtocol.makeSenderId;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public class Gilmour<T extends GilmourBackend> implements GilmourRequestExecutor {
    private final T backend;

    private class Subscribers extends HashMap<String, ArrayList<GilmourSubscription>> {}
    private Subscribers subscribers = new Subscribers();
    private boolean enableHealthChecks = false;
    private boolean enableErrorReporting = false;

    private UUID ident;

    protected static Logger logger;

    public Gilmour(T backend) {
        this.backend = backend;
    }

    public UUID getIdent() {
        return ident;
    }

    /**
     * Enables the listeners which respond to the health check pings.
     * The topic to listen to is configured in the backend
      * @throws InterruptedException
     */
    public void enableHealthChecks() throws InterruptedException {
        enableHealthChecks = true;
        ident = UUID.randomUUID();
        String topic = backend.healthTopic(ident.toString());
        subscribe(topic, (r, w) -> {
            ArrayList<String> topics = new ArrayList<>();
            topics.addAll(subscribers.keySet());
            topics.removeIf((t) -> {
                return (t.startsWith(backend.responseTopic("")) ||
                        t.startsWith(backend.healthTopic(ident.toString())));
            });
            w.respond(topics);
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        backend.registerIdent(ident);
    }


    /**
     * Enables error reporting via the backend
     * The mechanism to deliver the error reports is configured
     * in the individual backends
     * @return
     */
    public Gilmour enableErrorReporting() {
        this.enableErrorReporting = true;
        return this;
    }

    private boolean shouldReportErrors() {
        return enableErrorReporting;
    }

    /**
     *
     * @param topic The topic to subscribe to
     * @param h The callback handler
     * @param opts Options to configure the subscription
     * @return a subscription object which can be passed to the unsubscribe methods
     *          to de-register a handler
     * @throws InterruptedException
     */
    public GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts) throws InterruptedException {
        final ArrayList<GilmourSubscription> current = subscribers.get(topic);
        boolean needsSubscribe = (current == null || current.isEmpty());
        final GilmourSubscription sub = add_subscriber(topic, h, opts);
        if (needsSubscribe) {
            backend.subscribe(topic, opts);
        }
        return sub;
    }

    /**
     * Unregister a hander for a given topic
     * @param topic
     * @param s the subscription that needs to be removed
     */
    public void unsubscribe(String topic, GilmourSubscription s) {
        final ArrayList<GilmourSubscription> remaining = remove_subscriber(topic, s);
        if (remaining.isEmpty()) {
            backend.unsubscribe(topic);
        }
    }

    /**
     * Unregister all subscriptions for the given topic
     * @param topic
     */
    public void unsubscribe(String topic) {
        remove_subscribers(topic);
        backend.unsubscribe(topic);
    }


    public <T> String publish(String topic, T data) {
        final String sender = makeSenderId();
        this.publish(topic, data, 200, sender);
        return sender;
    }


    public <T> String publish(String topic, T data, GilmourHandler respHandler) throws InterruptedException {
        final String sender = makeSenderId();
        final String respChannel = backend.responseTopic(sender);
        final GilmourHandlerOpts opts = GilmourHandlerOpts.createGilmourHandlerOpts().setOneshot().setSendResponse(false);
        subscribe(respChannel, respHandler, opts);
        publish(topic, data, 200, sender);
        return sender;
    }

    protected <T> String publish(String topic, T data, int code) {
        final String sender = makeSenderId();
        this.publish(topic, data, code, sender);
        return sender;
    }

    protected <T> void publish(String topic, T data, int code, String sender) {
        final String message = GilmourProtocol.GilmourData.createGilmourData().
                setSender(sender).setData(data).setCode(code).render();
        logger.debug(message);
        backend.publish(topic, message);
    }

    @Override
    public void execSubscribers(String key, String topic, String message) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(key);
            if (subs == null) {
                logger.error("No subs found!! Key: " + key + ", Topic: " + topic);
                return;
            }
            for(Iterator<GilmourSubscription> si = subs.iterator(); si.hasNext();) {
                GilmourSubscription s = si.next();
                if (s.getOpts().isOneshot())
                    si.remove();
                this.executeSubscriber(s, topic, message);
            }
        }
    }

    public void executeSubscriber(GilmourSubscription sub, String topic, String data) {
        final GilmourProtocol.RecvGilmourData d = GilmourProtocol.parseJson(data);
        GilmourHandlerOpts opts = sub.getOpts();
        if (opts.getGroup() != null) {
            if (!backend.acquire_group_lock(opts.getGroup(), d.getSender()))
                return;
        }
        new Thread(() -> {
            this.handleRequest(sub, topic, d);
        }).start();
    }

    private void handleRequest(GilmourSubscription sub, String topic, GilmourProtocol.RecvGilmourData d) {
        GilmourRequest req = new GilmourRequest(topic, d);
        GilmourResponder res = new GilmourResponder(backend.responseTopic(d.getSender()));
        try {
            sub.getHandler().process(req, res);
        }
        catch (Exception e) {
            final GilmourProtocol.GilmourErrorResponse error = new GilmourProtocol.GilmourErrorResponse(500, d.getSender(),
                    topic, req.stringData(), e.getMessage(),
                    Arrays.toString(e.getStackTrace()));
            if (sub.getOpts().sendResponse())
                res.respond(error, 500);
            if (shouldReportErrors()) {
                backend.reportError(error);
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
            if (subs.isEmpty()) {
                subscribers.remove(topic);
            }
            return subs;
        }
    }

    protected void remove_subscribers(String topic) {
        synchronized (subscribers) {
            final ArrayList<GilmourSubscription> subs = subscribers.get(topic);
            subs.clear();
            subscribers.remove(topic);
        }
    }

    /**
     * Start the gilmour listener
     */
    public void start(Logger logger) {
        Gilmour.logger = logger;
        backend.start(this, logger);
    }

    public void start() {
        start(LogManager.getLogger());
    }

    public void stop() {
        if (enableHealthChecks) {
            backend.unRegisterIdent(ident);
        }
        backend.stop();
    }
}
