package run;

import gilmour.GilmourHandlerOpts;
import gilmour.Redis;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public class Run {
    static class Foo {
        String bar;
        int fubar;

        public Foo(String bar, int foo) {
            this.bar = bar;
            this.fubar = foo;
        }
    }
    public static void main(String[] args) {
        final Redis redis = new Redis();
        Foo f = new Foo("hello", 42);
        redis.subscribe("mytopic", (r, w) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            w.<Foo>respond(new Foo("topic response", 9));
        }, GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("unique"));

        redis.subscribe("mytopic*", (r, w) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            w.<Foo>respond(new Foo("wildcard response", 9));
        }, GilmourHandlerOpts.createGilmourHandlerOpts().setGroup("unique"));
        redis.start();
        for (int i = 0; i < 10; i++)
            redis.<Foo>publish("mytopic", f, (r, w) -> {
                Foo data = r.<Foo>data(Foo.class);
                System.out.println("Got response data:" + data.bar + ", " + data.fubar);
            });
    }
}
