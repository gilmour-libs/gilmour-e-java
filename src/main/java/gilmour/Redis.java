package gilmour;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */

class Subscription {
    private GilmourHandlerOpts opts;
    private GilmourHandler handler;

    public Subscription(GilmourHandler handler, GilmourHandlerOpts opts) {
        this.setHandler(handler);
        this.setOpts(opts);
    }
    public GilmourHandlerOpts getOpts() {
        return opts;
    }

    public void setOpts(GilmourHandlerOpts opts) {
        this.opts = opts;
    }

    public GilmourHandler getHandler() {
        return handler;
    }

    public void setHandler(GilmourHandler handler) {
        this.handler = handler;
    }
}

class GilmourData {
    private String sender;
    private int code;
    private Object userdata;

    private GilmourData() {
    }

    static GilmourData createGilmourData() {
        return new GilmourData();
    }

    public String getSender() {
        return sender;
    }

    public GilmourData setSender(String sender) {
        this.sender = sender;
        return this;
    }

    public int getCode() {
        return code;
    }

    public GilmourData setCode(int code) {
        this.code = code;
        return this;
    }

    public Object getUserdata() {
        return userdata;
    }

    public GilmourData setUserdata(Object userdata) {
        this.userdata = userdata;
        return this;
    }
}

class RedisGilmourRequest implements GilmourRequest {
    private String topic;
    private GilmourData gData;

    public RedisGilmourRequest(String topic, GilmourData gd) {
        this.topic = topic;
        this.gData = gd;
    }
    @Override
    public String sender() {
        return gData.getSender();
    }

    @Override
    public <T> T data() {
        return null;
    }
}

class RedisGilmourResponder implements GilmourResponder{
    private final String sender;
    private final String senderchannel;
    private Gilmour gilmourinst;

    public static String responseChannel(String sender) {
        return "response." + sender;
    }

    public RedisGilmourResponder(String sender, Gilmour gilmour) {
        this.sender = sender;
        this.gilmourinst = gilmour;
        this.senderchannel = responseChannel(sender);
    }

    @Override
    public <T> void send(T response) {
        gilmourinst.publish(senderchannel, response);
    }

    @Override
    public <T> void send(T response, int code) {
        gilmourinst.publish(senderchannel, response, code);
        // TODO: publish to redis
    }
}

public class Redis implements Gilmour {
    private String redishost;
    private int redisport;
    private RedisClient redis;
    private RedisPubSubConnection<String, String> pubsub;
    private Map<String, ArrayList<Subscription>> handlers;

    public Redis(String host, int port) {
        if (host == null) host = "127.0.0.1";
        if (port == 0) port = 4579;
        this.redishost = host;
        this.redisport = port;
        this.redis = new RedisClient(host, port);
        this.pubsub = redis.connectPubSub();
        setupListeners();
    }

    public Redis() {
        this("127.0.0.1", 4579);
    }

    public void subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts) {
        final Subscription sub = new Subscription(h, opts);
        if(handlers.get(topic) == null) {
            handlers.put(topic, new ArrayList<Subscription>());
        }
        handlers.get(topic).add(sub);
        if (topic.endsWith("*")) {
            pubsub.psubscribe(topic);
        } else {
            pubsub.subscribe(topic);
        }
    }

    public void unsubscribe(String topic, GilmourHandler h) {
        final ArrayList<Subscription> subs = handlers.get(topic);
        subs.remove(h);
        if (subs.isEmpty()) {
            pubsub.unsubscribe(topic);
        }
    }

    public void unsubscribe(String topic) {
        final ArrayList<Subscription> subs = handlers.get(topic);
        subs.clear();
        pubsub.unsubscribe(topic);

    }

    public <T> String publish(String topic, T data) {
        final String sender = makeSenderId();
        this.<T>publishWithSender(topic, data, 200, sender);
        return sender;
    }

    public <T> String publish(String topic, T data, int code) {
        final String sender = makeSenderId();
        this.<T>publishWithSender(topic, data, code, sender);
        return sender;
    }

    public <T> String publish(String topic, T data, GilmourHandler respHandler) {
        final String sender = makeSenderId();
        final String respChannel = RedisGilmourResponder.responseChannel(sender);
        final GilmourHandlerOpts opts = GilmourHandlerOpts.createGilmourHandlerOpts().setOneshot();
        subscribe(respChannel, respHandler, opts);
        publishWithSender(topic, data, 200, sender);
        return sender;
    }



    public void start() {

    }

    private void setupListeners() {
        final Redis self = this;
        pubsub.addListener(new RedisPubSubListener<String, String>() {
            @Override public void message(String s, String s2) {}
            @Override public void message(String s, String k1, String s2) {}
            @Override public void subscribed(String s, long l) {}
            @Override public void psubscribed(String s, long l) {}
            @Override public void unsubscribed(String s, long l) {}
            @Override public void punsubscribed(String s, long l) {}

            public void onMessage(String channel, String message) {
                self.processMessage(channel, null, message);
            }

            public void onPMessage(String pattern, String channel, String message) {
                self.processMessage(channel, pattern, message);
            }
        });

    }
    private String makeSenderId() {
        // TODO
        return "TODO";
    }

    private <T> void publishWithSender(String topic, T data, int code, String sender) {
        final GilmourData gd = GilmourData.createGilmourData().setSender(sender).setUserdata(data);
        // TODO: publish to sender
    }

    private void processMessage(String topic, String pattern, String message) {
        String key = null;
        if (pattern != null && !pattern.isEmpty()) {
            key = pattern;
        } else {
            key = topic;
        }
        final ArrayList<Subscription> subs = handlers.get(key);
        subs.forEach((s) -> {
            processRequest(s, topic, message);
        });
    }

    private void processRequest(Subscription sub, String topic, String data) {
        final GilmourData d = parseJson(data);
        final Gilmour self = this;
        new Thread(() -> {
            RedisGilmourRequest req = new RedisGilmourRequest(topic, d);
            RedisGilmourResponder res = new RedisGilmourResponder(d.getSender(), self);
            sub.getHandler().process(req, res);
            // TODO - Exception handling
            res.<EmptyResponse>send(new EmptyResponse());
        });
    }

    private GilmourData parseJson(String data) {
        //TODO: Jackson
        return null;
    }

    private class EmptyResponse {
    }


}
