package functional;

import gilmour.GilmourHandlerOpts;
import gilmour.GilmourSubscription;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by aditya@datascale.io on 27/05/15.
 */
public class WildcardTest extends BaseTest {
    @Test
    public void topicReceiveTest() {
        final String topic = "testtopic";
        final String wildTopic = topic + ".*";
        TestData received = new TestData();
        final Object lock = new Object();
        GilmourSubscription sub = redis.subscribe(wildTopic, (r, w) -> {
            TestData td = r.data(TestData.class);
            logger.debug("Received data: " + td.strval);
            received.strval = td.strval;
            received.intval = td.intval;
            synchronized (lock) {
                lock.notifyAll();
            }
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        synchronized (lock) {
            redis.publish(topic + ".foo", sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        redis.unsubscribe(wildTopic, sub);
        Assert.assertEquals(received.intval, sent.intval);
        Assert.assertEquals(received.strval, sent.strval);
    }

    @Test
    public void topicResponsedTest() {
        final String topic = "testtopic";
        final String wildTopic = topic + ".*";
        TestData received = new TestData();
        final Object lock = new Object();
        GilmourSubscription sub = redis.subscribe(wildTopic, (r, w) -> {
            TestData td = r.data(TestData.class);
            w.respond(new TestData(td.strval, td.intval + 1));
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        redis.publish(topic + ".foo", sent, (r,w) -> {
            TestData td = r.data(TestData.class);
            received.intval = td.intval;
            received.strval = td.strval;
            synchronized (lock) {
                lock.notifyAll();
            }
        });
        synchronized (lock) {
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        redis.unsubscribe(wildTopic, sub);
        Assert.assertEquals(received.intval, sent.intval + 1);
        Assert.assertEquals(received.strval, sent.strval);
    }
}
