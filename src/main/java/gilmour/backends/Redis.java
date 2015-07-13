package gilmour.backends;

import com.google.gson.Gson;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.protocol.SetArgs;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import gilmour.GilmourBackend;
import gilmour.GilmourHandlerOpts;
import gilmour.GilmourRequestExecutor;
import gilmour.protocol.GilmourProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Redis implements GilmourBackend {
    private static String defaultErrorQueue = "gilmour.errorqueue";
    private static String defaultErrorTopic = "gilmour.errors";
    private static String defaultHealthTopic = "gilmour.health";
    private static String defaultIdentKey = "gilmour.known_host.health";
    static String defaultResponseTopic = "gilmour.response";
    private GilmourRequestExecutor requestExecutor;
    private Logger logger;


    public enum errorMethods {QUEUE, PUBLISH}


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
        setErrorMethod(errorMethods.PUBLISH);
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
    public void reportError(GilmourProtocol.GilmourErrorResponse message) {
        Gson gson = new Gson();
        String errMsg = gson.toJson(message);
        switch (this.errorMethod) {
            case PUBLISH:
                publish(this.errorTopic, errMsg);
                break;
            case QUEUE:
                redisconnection.lpush(this.errorQueue, errMsg);
                redisconnection.ltrim(this.errorQueue, 0, 9999);
                break;
        }
    }

    @Override
    public void subscribe(String topic, GilmourHandlerOpts opts) throws InterruptedException {
        RedisFuture<Void> psubscribe;
        if (topic.endsWith("*")) {
            psubscribe = pubsub.psubscribe(topic);
        } else {
            psubscribe = pubsub.subscribe(topic);
        }
        psubscribe.await(1, TimeUnit.SECONDS);
    }

    @Override
    public void unsubscribe(String topic) {
        pubsub.unsubscribe(topic);
    }

    public String responseTopic(String sender) {
        return defaultResponseTopic + "." + sender;
    }

    @Override
    public String healthTopic(String ident) {
        return defaultHealthTopic + "." + ident;
    }

    @Override
    public void registerIdent(UUID uuid) {
        redisconnection.hset(defaultIdentKey, uuid.toString(), "true");
    }

    @Override
    public void unRegisterIdent(UUID uuid) {
        redisconnection.hdel(defaultIdentKey, uuid.toString());
    }

    public void publish(String topic, String message) {
        redisconnection.publish(topic, message);
    }


    @Override
    public void start(GilmourRequestExecutor e, Logger l) {
        if (l == null) {
            this.logger = LogManager.getLogger();
        }
        this.requestExecutor = e;
        this.logger = l;
        setupListeners();
    }

    @Override
    public void stop() {

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
        this.requestExecutor.execSubscribers(key, topic, message);
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
