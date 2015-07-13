package gilmour;

/**
 * Created by aditya@datascale.io on 13/07/15.
 */
public interface GilmourRequestExecutor {
    void execSubscribers(String key, String topic, String message);
}
