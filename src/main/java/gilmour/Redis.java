package gilmour;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.protocol.SetArgs;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;


class RedisGilmourRequest implements GilmourRequest {
    protected final String topic;
    protected final GilmourProtocol.RecvGilmourData gData;

    public RedisGilmourRequest(String topic, GilmourProtocol.RecvGilmourData gd) {
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

    @Override
    public String stringData() {
        return gData.toString();
    }
}


class RedisGilmourResponder implements GilmourResponder{
    static String defaultResponseTopic = "gilmour.response";
    private final String senderchannel;
    private Object message = null;
    private int code = 0;

    public boolean isResponseSent() {
        return responseSent;
    }

    private boolean responseSent = false;

    public static String responseChannel(String sender) {
        return defaultResponseTopic + "." + sender;
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

public class Redis extends Gilmour {
    private static String defaultErrorQueue = "gilmour.errorqueue";
    private static String defaultErrorTopic = "gilmour.errors";


    public enum errorMethods {QUEUE, PUBLISH, NONE}

    private static final Logger logger = LogManager.getLogger();
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
                redisconnection.append(this.errorQueue, message.toString());
                break;
        }
    }

    @Override
    public GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts) {
        GilmourSubscription sub = super.add_subscriber(topic, h, opts);
        if (topic.endsWith("*")) {
            pubsub.psubscribe(topic);
        } else {
            pubsub.subscribe(topic);
        }
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
        return RedisGilmourResponder.responseChannel(sender);
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
    public boolean isAResponse(String topic) {
        return topic.startsWith(RedisGilmourResponder.defaultResponseTopic);
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
