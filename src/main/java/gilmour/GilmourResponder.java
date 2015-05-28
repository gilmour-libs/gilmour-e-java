package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourResponder {
    <T> void respond(T response);

    <T> void respond(T response, int code);

    void send(Gilmour gilmour);
}
