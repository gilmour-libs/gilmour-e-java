package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface Gilmour {
    GilmourSubscription subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts);
    void unsubscribe(String topic, GilmourSubscription h);
    void unsubscribe(String topic);
    <T> String publish(String topic, T data);
    <T> String publish(String topic, T data, int code);
    <T> String publish(String topic, T data, GilmourHandler respHandler);
    void start();


}
