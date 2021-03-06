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
    public void doubleSubscriberTest() throws InterruptedException {
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
        GilmourSubscription sub1 = gilmour.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = gilmour.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("command", 0);
        synchronized (lock) {
            gilmour.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(topic, sub1);
        gilmour.unsubscribe(topic, sub2);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }

    @Test
    public void tripleSubscriberTest() throws InterruptedException {
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
        GilmourSubscription sub1 = gilmour.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = gilmour.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub3 = gilmour.subscribe(topic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("command", 0);
        synchronized (lock) {
            gilmour.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(topic, sub1);
        gilmour.unsubscribe(topic, sub2);
        gilmour.unsubscribe(topic, sub3);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }

    @Test
    public void doubleWildcardTest() throws InterruptedException {
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
        GilmourSubscription sub1 = gilmour.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub2 = gilmour.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        GilmourSubscription sub3 = gilmour.subscribe(wildTopic, handler,
                GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("testgroup"));
        TestData sent = new TestData("doubleWildcardTest", 0);
        synchronized (lock) {
            gilmour.publish(topic + ".foo", sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(wildTopic, sub1);
        gilmour.unsubscribe(wildTopic, sub2);
        gilmour.unsubscribe(wildTopic, sub3);
        Assert.assertEquals(received.size(), 1);
        TestData d = received.get(0);
        Assert.assertEquals(d.intval, sent.intval);
        Assert.assertEquals(d.strval, sent.strval);
    }
}
