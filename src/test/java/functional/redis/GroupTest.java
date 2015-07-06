package functional.redis;

import gilmour.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Created by aditya@datascale.io on 27/05/15.
 */
public class GroupTest extends BaseTest {
    @Test
    public void doubleSubscriberTest() {
        final String topic = "testtopic";
        ArrayList<TestData> received = new ArrayList<>();
        final Object lock = new Object();
        GilmourHandler handler = (r, w) -> {
            TestData td = r.data(TestData.class);
            logger.debug("Received data: " + td.strval);
            received.add(td);
            synchronized (lock) {
                lock.notifyAll();
            }
        };
        GilmourSubscription sub1 = redis.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = redis.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("command", 0);
        synchronized (lock) {
            redis.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        redis.unsubscribe(topic, sub1);
        redis.unsubscribe(topic, sub2);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }

    @Test
    public void tripleSubscriberTest() {
        final String topic = "testtopic";
        ArrayList<TestData> received = new ArrayList<>();
        final Object lock = new Object();
        GilmourHandler handler = (r, w) -> {
            TestData td = r.data(TestData.class);
            logger.debug("Received data: " + td.strval);
            received.add(td);
            synchronized (lock) {
                lock.notifyAll();
            }
        };
        GilmourSubscription sub1 = redis.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = redis.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub3 = redis.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("command", 0);
        synchronized (lock) {
            redis.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        redis.unsubscribe(topic, sub1);
        redis.unsubscribe(topic, sub2);
        redis.unsubscribe(topic, sub3);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }

    @Test
    public void doubleWildcardTest() {
        final String topic = "testtopic";
        final String wildTopic = topic + ".*";
        ArrayList<TestData> received = new ArrayList<>();
        final Object lock = new Object();
        GilmourHandler handler = (r, w) -> {
            TestData td = r.data(TestData.class);
            logger.debug("Received data: " + td.strval);
            received.add(td);
            synchronized (lock) {
                lock.notifyAll();
            }
        };
        GilmourSubscription sub1 = redis.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = redis.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub3 = redis.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("doubleWildcardTest", 0);
        synchronized (lock) {
            redis.publish(topic + ".foo", sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        redis.unsubscribe(wildTopic, sub1);
        redis.unsubscribe(wildTopic, sub2);
        redis.unsubscribe(wildTopic, sub3);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }
}
