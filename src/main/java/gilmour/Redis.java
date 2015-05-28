package gilmour;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.protocol.SetArgs;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import static java.util.UUID.randomUUID;

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
    protected final String topic;
    protected final RecvGilmourData gData;

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
        return this.gData.getData(cls);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int code() {
        return gData.getCode();
    }
}


class RedisGilmourResponder implements GilmourResponder{
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
        if (responseSent) return;
        if (code == 0)
            gilmourinst.publish(senderchannel, message);
        else
            gilmourinst.publish(senderchannel, message, code);
        responseSent = true;
    }
}

public class Redis implements Gilmour {
    private static final Logger logger = LogManager.getLogger();
    private final RedisConnection<String, String> redisconnection;
    private final RedisClient redis;
    private final RedisPubSubConnection<String, String> pubsub;
    private final HashMap<String, ArrayList<GilmourSubscription>> handlers;

    public Redis(String host, int port) {
        if (host == null) host = "127.0.0.1";
        int defaultport = 6379;
        if (port == 0) port = defaultport;
        String redishost = host;
        int redisport = port;
        this.redis = new RedisClient(host, port);
        this.pubsub = redis.connectPubSub();
        this.redisconnection = redis.connect();
        this.handlers = new HashMap<>();
    }

    public Redis() {
        this("127.0.0.1", 0);
    }

    public GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts) {
        final GilmourSubscription sub = new GilmourSubscription(h, opts);
        synchronized (handlers) {
            if (handlers.get(topic) == null) {
                handlers.put(topic, new ArrayList<>());
            }
            handlers.get(topic).add(sub);
        }
        if (topic.endsWith("*")) {
            pubsub.psubscribe(topic);
        } else {
            pubsub.subscribe(topic);
        }
        return sub;
    }

    public void unsubscribe(String topic, GilmourSubscription s) {
        synchronized (handlers) {
            final ArrayList<GilmourSubscription> subs = handlers.get(topic);
            subs.remove(s);
            if (subs.isEmpty()) {
                pubsub.unsubscribe(topic);
            }
        }
    }

    public void unsubscribe(String topic) {
        synchronized (handlers) {
            final ArrayList<GilmourSubscription> subs = handlers.get(topic);
            subs.clear();
            pubsub.unsubscribe(topic);
        }
    }

    public <T> String publish(String topic, T data) {
        final String sender = makeSenderId();
        this.publishWithSender(topic, data, 200, sender);
        return sender;
    }

    public <T> String publish(String topic, T data, int code) {
        final String sender = makeSenderId();
        this.publishWithSender(topic, data, code, sender);
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
            @Override
            public void subscribed(String s, long l) {
            }

            @Override
            public void psubscribed(String s, long l) {
            }

            @Override
            public void unsubscribed(String s, long l) {
            }

            @Override
            public void punsubscribed(String s, long l) {
            }

            @Override
            public void message(String channel, String message) {
                logger.debug("Got message:" + message);
                self.processMessage(channel, null, message);
            }

            @Override
            public void message(String pattern, String channel, String message) {
                logger.debug("Got message:" + message);
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
        logger.debug(message);
        redisconnection.publish(topic, message);
    }

    private void processMessage(String topic, String pattern, String message) {
        String key;
        if (pattern != null && !pattern.isEmpty()) {
            key = pattern;
        } else {
            key = topic;
        }
        synchronized (handlers) {
            final ArrayList<GilmourSubscription> subs = handlers.get(key);
            for(Iterator<GilmourSubscription> si = subs.iterator(); si.hasNext();) {
                GilmourSubscription s = si.next();
                if (s.getOpts().isOneshot())
                    si.remove();
                processRequest(s, topic, message);
            }
        }

    }

    private void processRequest(GilmourSubscription sub, String topic, String data) {
        final RecvGilmourData d = parseJson(data);
        GilmourHandlerOpts opts = sub.getOpts();
        if (opts.getGroup() != null) {
            if (!acquire_group_lock(opts.getGroup(), d.getSender()))
                return;
        }
        new Thread(() -> {
            this.doRequestHandler(sub, topic, d);
        }).start();
    }

    private void doRequestHandler(GilmourSubscription sub, String topic, RecvGilmourData d) {
        RedisGilmourRequest req = new RedisGilmourRequest(topic, d);
        RedisGilmourResponder res = new RedisGilmourResponder(d.getSender());
        try {
            sub.getHandler().process(req, res);
        }
        catch (Exception e) {
            if (!topic.startsWith("response"))
                res.respond(new GilmourErrorResponse(e.getMessage(),
                        Arrays.toString(e.getStackTrace())), 500);
        }
        finally {
            if (!topic.startsWith("response"))
                res.send(this);
        }
    }

    private boolean acquire_group_lock(String group, String sender) {
        String key = sender + group;
        SetArgs setargs = SetArgs.Builder.ex(600).nx();
        String resp = this.redisconnection.set(key, key, setargs);
        logger.debug("Lock acquire response: " + resp);

        return (resp != null && resp.equals("OK"));
    }

    private RecvGilmourData parseJson(String data) {
        Gson gson = new Gson();
        return gson.fromJson(data, RecvGilmourData.class);
    }

    private class GilmourErrorResponse {
        private final String message;
        private final String stacktrace;

        public GilmourErrorResponse(String message, String stacktrace) {
            this.message = message;
            this.stacktrace = stacktrace;
        }
    }

    private class GilmourEmptyResponse {}


}
