package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourResponder {
    public <T> void send(T response);
    public <T> void send(T response, int code);
}
