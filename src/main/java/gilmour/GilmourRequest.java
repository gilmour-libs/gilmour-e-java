package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourRequest {
    public String sender();
    public <T> T data();
}
