package gilmour;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.protocol.SetArgs;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;

import java.util.*;
import java.util.logging.Logger;

import static java.util.UUID.*;

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
    private Object data;

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

    public Object getData() {
        return data;
    }

    public GilmourData setData(Object data) {
        this.data = data;
        return this;
    }

}

class RecvGilmourData {
    private String sender;
    private int code;
    private JsonElement data;

    public RecvGilmourData() {}
    public <T> T getData(Class<T> cls) {
        Gson gson = new Gson();
        return gson.fromJson(data, cls);
    }

    public String getSender() {
        return sender;
    }

    public int getCode() {
        return code;
    }
}

class RedisGilmourRequest implements GilmourRequest {
    private String topic;
    private RecvGilmourData gData;

    public RedisGilmourRequest(String topic, RecvGilmourData gd) {
        this.topic = topic;
        this.gData = gd;
    }
    @Override
    public String sender() {
        return gData.getSender();
    }

    @Override
    public <T> T data(Class<T> cls) {
        return this.gData.<T>getData(cls);
    }
}

class RedisGilmourResponder implements GilmourResponder{
    private final String sender;
    private final String senderchannel;
    private Object message = null;
    private int code = 0;

    public boolean isResponseSent() {
        return responseSent;
    }

    private boolean responseSent = false;

    public static String responseChannel(String sender) {
        return "response." + sender;
    }

    public RedisGilmourResponder(String sender) {
        this.sender = sender;
        this.senderchannel = responseChannel(sender);
    }

    @Override
    public <T> void respond(T response) {
        message = response;
    }

    @Override
    public <T> void respond(T response, int code) {
        message = response;
        this.code = code;
    }

    @Override
    public void send(Gilmour gilmourinst) {
        if (code == 0)
            gilmourinst.publish(senderchannel, message);
        else
            gilmourinst.publish(senderchannel, message, code);
    }
}

public class Redis implements Gilmour {
    private final int defaultport = 6379;
    private final RedisConnection<String, String> redisconnection;
    private String redishost;
    private int redisport;
    private RedisClient redis;
    private RedisPubSubConnection<String, String> pubsub;
    private HashMap<String, ArrayList<Subscription>> handlers;

    public Redis(String host, int port) {
        if (host == null) host = "127.0.0.1";
        if (port == 0) port = defaultport;
        this.redishost = host;
        this.redisport = port;
        this.redis = new RedisClient(host, port);
        this.pubsub = redis.connectPubSub();
        this.redisconnection = redis.connect();
        this.handlers = new HashMap<>();
    }

    public Redis() {
        this("127.0.0.1", 0);
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
        setupListeners();
    }

    private void setupListeners() {
        final Redis self = this;
        pubsub.addListener(new RedisPubSubListener<String, String>() {
            @Override public void subscribed(String s, long l) {}
            @Override public void psubscribed(String s, long l) {}
            @Override public void unsubscribed(String s, long l) {}
            @Override public void punsubscribed(String s, long l) {}

            @Override
            public void message(String channel, String message) {
                Logger.getGlobal().info("Got message:" + message);
                self.processMessage(channel, null, message);
            }

            @Override
            public void message(String pattern, String channel, String message) {
                Logger.getGlobal().info("Got message:" + message);
                self.processMessage(channel, pattern, message);
            }
        });

    }
    private String makeSenderId() {
        return randomUUID().toString();
    }

    private <T> void publishWithSender(String topic, T data, int code, String sender) {
        final GilmourData gd = GilmourData.createGilmourData().setSender(sender).setData(data)
                .setCode(code);
        Gson gson = new Gson();
        final String message = gson.toJson(gd);
        Logger.getGlobal().info(message);
        redisconnection.publish(topic, message);
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
            if (s.getOpts().isOneshot())
                handlers.remove(s);
            processRequest(s, topic, message);
        });
    }

    private void processRequest(Subscription sub, String topic, String data) {
        final RecvGilmourData d = parseJson(data);
        GilmourHandlerOpts opts = sub.getOpts();
        if (opts.getGroup() != null) {
            if (!acquire_group_lock(opts.getGroup(), d.getSender()))
                return;
        }
        final Gilmour self = this;
        new Thread(() -> {
            RedisGilmourRequest req = new RedisGilmourRequest(topic, d);
            RedisGilmourResponder res = new RedisGilmourResponder(d.getSender());
            try {
                sub.getHandler().process(req, res);
            }
            catch (Exception e) {
                res.<GilmourErrorResponse>respond(new GilmourErrorResponse(e.getMessage(),
                        e.getStackTrace().toString()), 500);
            }
            finally {
                if (!topic.startsWith("response"))
                    res.send(self);
            }
        }).start();
    }

    private boolean acquire_group_lock(String group, String sender) {
        String key = sender + group;
        SetArgs setargs = SetArgs.Builder.ex(600).nx();
        String resp = this.redisconnection.set(key, key, setargs);
        Logger.getGlobal().info("Lock acquire response: " + resp);
        Boolean gotit = (resp != null && resp.equals("OK"));
//        if (!gotit)
//            Logger.getGlobal().info("Did not get lock for " + key);
        return gotit;
    }

    private RecvGilmourData parseJson(String data) {
        Gson gson = new Gson();
        return gson.fromJson(data, RecvGilmourData.class);
    }

    private class GilmourErrorResponse {
        private String message;
        private String stacktrace;

        public GilmourErrorResponse(String message, String stacktrace) {
            this.message = message;
            this.stacktrace = stacktrace;
        }
    }

    private class GilmourEmptyResponse {}


}
