package functional.redis;

import gilmour.GilmourHandlerOpts;
import gilmour.GilmourPublishOpts;
import gilmour.GilmourSubscription;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by aditya@datascale.io on 27/05/15.
 */

public class TopicTest extends BaseTest {

    class RecvData {
        public String sender;
        public String topic;
        public int code;
    }

    @Test
    public void topicReceiveTest() throws InterruptedException {
        final String topic = "testrecvtopic";
        TestData received = new TestData();
        final Object lock = new Object();

        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
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
            gilmour.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(topic, sub);
        Assert.assertEquals(received.intval, sent.intval);
        Assert.assertEquals(received.strval, sent.strval);
    }

    @Test
    public void requestTest() throws InterruptedException {
        final String topic = "testreqtopic";
        final Object lock = new Object();
        RecvData recvData = new RecvData();
        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
            recvData.sender = r.sender();
            recvData.topic = r.topic();
            synchronized (lock) {
                lock.notifyAll();
            }
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        String sender;
        synchronized (lock) {
            sender = gilmour.publish(topic, sent);
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(topic, sub);
        Assert.assertEquals(recvData.sender, sender);
        Assert.assertEquals(recvData.topic, topic);
    }

    @Test
    public void topicGoodResponseTest() throws InterruptedException {
        final String topic = "testresptopic";
        TestData received = new TestData();
        RecvData rd = new RecvData();
        final Object lock = new Object();
        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
            TestData td = r.data(TestData.class);
            w.respond(new TestData(td.strval, td.intval + 1));
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        gilmour.publish(topic, sent, (r, w) -> {
            TestData td = r.data(TestData.class);
            received.intval = td.intval;
            received.strval = td.strval;
            rd.code = r.code();
            synchronized (lock) {
                lock.notifyAll();
            }
        }, GilmourPublishOpts.createGilmourPublishOpts().setConfirmSubscribers());
        synchronized (lock) {
            try {
                lock.wait(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        gilmour.unsubscribe(topic, sub);
        Assert.assertEquals(received.intval, sent.intval + 1);
        Assert.assertEquals(received.strval, sent.strval);
        Assert.assertEquals(rd.code, 200);
    }

    @Test
    public void topicCustomCodeTest() throws InterruptedException {
        final String topic = "testcustomtopic";
        final int respcode = 400;
        RecvData rd = new RecvData();
        final Object lock = new Object();
        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
            TestData td = r.data(TestData.class);
            w.respond(new TestData(td.strval, td.intval + 1), respcode);
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        gilmour.publish(topic, sent, (r, w) -> {
            rd.code = r.code();
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
        gilmour.unsubscribe(topic, sub);
        Assert.assertEquals(rd.code, respcode);
    }

    @Test
    public void failedHandlerTest() throws InterruptedException {
        final String topic = "testfailtopic";
        final int respcode = 400;
        RecvData rd = new RecvData();
        final Object lock = new Object();
        GilmourSubscription sub = gilmour.subscribe(topic, (r, w) -> {
            TestData td = r.data(TestData.class);
            w.respond(new TestData(td.strval, td.intval + 1), respcode);
            throw new RuntimeException();
        }, GilmourHandlerOpts.createGilmourHandlerOpts());
        TestData sent = new TestData("command", 0);
        gilmour.publish(topic, sent, (r, w) -> {
            rd.code = r.code();
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
        gilmour.unsubscribe(topic, sub);
        Assert.assertEquals(rd.code, 500);
    }
}
