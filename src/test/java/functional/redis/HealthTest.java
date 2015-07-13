package functional.redis;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import gilmour.*;
import gilmour.protocol.GilmourProtocol;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Created by aditya@datascale.io on 06/07/15.
 */
public class HealthTest extends BaseTest {
    private RedisClient errredis;
    private RedisPubSubConnection<String, String> errpubsub;

    @BeforeClass
    public void setupHealth() {
        this.gilmour.enableErrorReporting();
        this.errredis = new RedisClient("127.0.0.1", 6379);
        this.errpubsub = errredis.connectPubSub();
        this.errredis.connect();
    }

    @Test
    public void errorTest() throws InterruptedException {
        final String topic = "testerrtopic";
        ArrayList<Integer> received = new ArrayList<>();
        final Object resplock = new Object();
        final Object errlock = new Object();
        ArrayList<GilmourProtocol.GilmourErrorResponse> errors = new ArrayList<>();

        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
            throw new Exception("Simulated error");
        }, GilmourHandlerOpts.createGilmourHandlerOpts());

        GilmourHandler resHandler = (r, w) -> {
            received.add(r.code());
            synchronized (resplock) {
                resplock.notifyAll();
            }
        };

        errpubsub.addListener(new RedisPubSubListener<String, String>() {
            @Override
            public void message(String ch, String data) {
                try {
                    Gson gson = new Gson();
                    GilmourProtocol.GilmourErrorResponse err = gson.fromJson(data,GilmourProtocol.GilmourErrorResponse.class);
                    logger.debug("Received data: " + err);
                    errors.add(err);
                } catch (Exception e) {
                    System.err.println("Cannot parse error channel message");
                }
                synchronized (errlock) {
                    errlock.notifyAll();
                }
            }
            @Override
            public void message(String s, String k1, String s2) {}
            @Override
            public void subscribed(String s, long l) {}
            @Override
            public void psubscribed(String s, long l) {}
            @Override
            public void unsubscribed(String s, long l) {}
            @Override
            public void punsubscribed(String s, long l) {}
        });
        errpubsub.subscribe("gilmour.errors");

        synchronized (resplock) {
            gilmour.publish(topic, "Error test", resHandler);
            resplock.wait(1000);
        }
        synchronized (errlock) {
            errlock.wait(1000);
        }
        Assert.assertFalse(errors.isEmpty());
        Assert.assertEquals(errors.get(0).getCode(), 500);
        Assert.assertEquals(errors.get(0).getTopic(), topic);

        gilmour.unsubscribe(topic, sub);
        errpubsub.unsubscribe("gilmour.errors");
    }

    @Test
    public void healthTest() throws InterruptedException {
        gilmour.enableHealthChecks();
        String topic = "healthtest.topic";
        gilmour.subscribe(topic, (r, w) -> {
            // do nothing
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        final Object resplock = new Object();
        final ArrayList<String> retTopics = new ArrayList<>();

        gilmour.publish("gilmour.health." + gilmour.getIdent().toString(), "ping",
                (r, w) -> {
                    TypeToken<ArrayList<String>> typeTok = new TypeToken<ArrayList<String>>() {};
                    ArrayList<String> topics = r.data(typeTok.getType());
                    System.err.println(topics);
                    retTopics.addAll(topics);
                    synchronized (resplock) {
                        resplock.notifyAll();
                    }
                });
        synchronized (resplock) {
            resplock.wait(10000);
        }
        Assert.assertTrue(retTopics.contains(topic));
    }
}
