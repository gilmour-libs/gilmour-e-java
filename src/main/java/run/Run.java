package run;

import gilmour.GilmourHandlerOpts;
import gilmour.Redis;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */

public class Run {
    public static void main(String[] args) throws InterruptedException {
        final Redis redis = new Redis();
        redis.subscribe("echo*", (r, w) -> {
            w.respond("Pong");
        }, GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("unique"));
        redis.subscribe("echo*", (r, w) -> {
            w.respond("Pong");
        }, GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("unique"));
        redis.start();
    }
}
