package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourHandler {
    void process(GilmourRequest r, GilmourResponder w);
}
