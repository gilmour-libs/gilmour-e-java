package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourRequest {
    public String sender();
    <T> T data(Class<T> cls);

    String topic();
    int code();
}
