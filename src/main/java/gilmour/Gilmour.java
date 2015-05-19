package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface Gilmour {
    public void subscribe(String topic, GilmourHandler h, GilmourHandlerOpts opts);
    public void unsubscribe(String topic, GilmourHandler h);
    public void unsubscribe(String topic);
    public <T> String publish(String topic, T data);
    public <T> String publish(String topic, T data, int code);
    public <T> String publish(String topic, T data, GilmourHandler respHandler);
    public void start();


}
