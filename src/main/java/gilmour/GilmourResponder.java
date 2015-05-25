package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourResponder {
    public <T> void respond(T response);

    public <T> void respond(T response, int code);

    public void send(Gilmour gilmour);
}
