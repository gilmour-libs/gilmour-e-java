package gilmour;

import com.google.gson.Gson;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.protocol.SetArgs;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Redis extends Gilmour {
    private static String defaultErrorQueue = "gilmour.errorqueue";
    private static String defaultErrorTopic = "gilmour.errors";
    private static String defaultHealthTopic = "gilmour.health";
    private static String defaultIdentKey = "gilmour.known_host.health";
    static String defaultResponseTopic = "gilmour.response";


    public enum errorMethods {QUEUE, PUBLISH, NONE}


    private final RedisConnection<String, String> redisconnection;
    private final RedisClient redis;
    private final RedisPubSubConnection<String, String> pubsub;
    private errorMethods errorMethod;
    private String errorQueue;
    private String errorTopic;



    public Redis(String host, int port) {
        if (host == null) host = "127.0.0.1";
        int defaultport = 6379;
        if (port == 0) port = defaultport;
        String redishost = host;
        int redisport = port;
        this.redis = new RedisClient(host, port);
        this.pubsub = redis.connectPubSub();
        this.redisconnection = redis.connect();
        errorMethod = errorMethods.NONE;
    }

    public Redis() {
        this("127.0.0.1", 0);
    }

    public String getErrorTopic() {
        return errorTopic;
    }

    public Redis setErrorTopic(String errorTopic) {
        this.errorTopic = errorTopic;
        return this;
    }


    public String getErrorQueue() {
        return errorQueue;
    }

    public Redis setErrorQueue(String errorQueue) {
        this.errorQueue = errorQueue;
        return this;
    }

    public Redis.errorMethods getErrorMethod() {
        return errorMethod;
    }

    public Redis setErrorMethod(Redis.errorMethods errorMethod) {
        this.errorMethod = errorMethod;
        switch(errorMethod) {
            case QUEUE:
                errorQueue = defaultErrorQueue;
                break;
            case PUBLISH:
                errorTopic = defaultErrorTopic;
                break;
        }
        return this;
    }

    @Override
    public boolean canReportErrors() {
        return this.errorMethod != Redis.errorMethods.NONE;
    }

    @Override
    public void reportError(GilmourProtocol.GilmourErrorResponse message) {
        switch (this.errorMethod) {
            case PUBLISH:
                publish(this.errorTopic, message);
                break;
            case QUEUE:
                Gson gson = new Gson();
                redisconnection.lpush(this.errorQueue, gson.toJson(message));
                redisconnection.ltrim(this.errorQueue, 0, 9999);
                break;
        }
    }

    @Override
    public GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts) throws InterruptedException {
        GilmourSubscription sub = super.add_subscriber(topic, h, opts);
        RedisFuture<Void> psubscribe;
        if (topic.endsWith("*")) {
            psubscribe = pubsub.psubscribe(topic);
        } else {
            psubscribe = pubsub.subscribe(topic);
        }
        psubscribe.await(1, TimeUnit.SECONDS);
        return sub;
    }

    @Override
    public void unsubscribe(String topic, GilmourSubscription s) {
        final ArrayList<GilmourSubscription> remaining = super.remove_subscriber(topic, s);
        if (remaining.isEmpty()) {
            pubsub.unsubscribe(topic);
        }
    }

    @Override
    public void unsubscribe(String topic) {
        super.remove_subscribers(topic);
        pubsub.unsubscribe(topic);
    }

    public String responseTopic(String sender) {
        return defaultResponseTopic + "." + sender;
    }

    @Override
    protected String healthTopic(String ident) {
        return defaultHealthTopic + "." + ident;
    }

    @Override
    protected void registerIdent(UUID uuid) {
        redisconnection.hset(defaultIdentKey, uuid.toString(), "true");
    }

    @Override
    protected void unRegisterIdent(UUID uuid) {
        redisconnection.hdel(defaultIdentKey, uuid.toString());
    }

    public <T> void publish(String topic, T data, int code, String sender) {
        final String message = GilmourProtocol.GilmourData.createGilmourData().
                setSender(sender).setData(data).setCode(code).render();
        logger.debug(message);
        redisconnection.publish(topic, message);
    }


    @Override
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

    private void processMessage(String topic, String pattern, String message) {
        String key;
        if (pattern != null && !pattern.isEmpty()) {
            key = pattern;
        } else {
            key = topic;
        }
        execSubscribers(key, topic, message);
    }

    @Override
    public boolean acquire_group_lock(String group, String sender) {
        String key = sender + group;
        SetArgs setargs = SetArgs.Builder.ex(600).nx();
        String resp = this.redisconnection.set(key, key, setargs);
        logger.debug("Lock acquire response: " + resp);

        return (resp != null && resp.equals("OK"));
    }
}
