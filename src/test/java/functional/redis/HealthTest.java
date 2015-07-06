package functional.redis;

import gilmour.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Created by aditya@datascale.io on 06/07/15.
 */
public class HealthTest extends BaseTest {
    @BeforeClass
    public void setupHealth() {
        this.redis.setErrorMethod(Redis.errorMethods.PUBLISH);
    }

    @Test
    public void errorTest() throws InterruptedException {
        final String topic = "testtopic";
        ArrayList<Integer> received = new ArrayList<>();
        final Object resplock = new Object();
        final Object errlock = new Object();
        ArrayList<GilmourProtocol.GilmourErrorResponse> errors = new ArrayList<>();

        GilmourSubscription sub = redis.subscribe(topic, (r, w) -> {
            throw new Exception("Simulated error");
        }, GilmourHandlerOpts.createGilmourHandlerOpts());

        GilmourHandler resHandler = (r, w) -> {
            received.add(r.code());
            synchronized (resplock) {
                resplock.notifyAll();
            }
        };

        GilmourSubscription errsub = redis.subscribe("gilmour.errors", (r, w) -> {
            try {
                GilmourProtocol.GilmourErrorResponse err = r.data(GilmourProtocol.GilmourErrorResponse.class);
                logger.debug("Received data: " + err);
                errors.add(err);
            }
            catch (Exception e) {
                System.err.println("Cannot parse error channel message");
            }
            synchronized (errlock) {
                errlock.notifyAll();
            }
        }, GilmourHandlerOpts.createGilmourHandlerOpts().setSendResponse(false));

        synchronized (resplock) {
            redis.publish(topic, "Error test", resHandler);
            resplock.wait(10000);
        }
        synchronized (errlock) {
            errlock.wait(10000);
        }
        Assert.assertFalse(errors.isEmpty());
        Assert.assertEquals(errors.get(0).getCode(), 500);
        Assert.assertEquals(errors.get(0).getTopic(), topic);

        redis.unsubscribe(topic, sub);
        redis.unsubscribe("gilmour.errors", errsub);
    }
}
