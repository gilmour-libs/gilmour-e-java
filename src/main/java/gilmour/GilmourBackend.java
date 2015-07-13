package gilmour;

import gilmour.protocol.GilmourProtocol;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

/**
 * Created by aditya@datascale.io on 13/07/15.
 */
public interface GilmourBackend {
    void subscribe(String topic, GilmourHandlerOpts opts) throws InterruptedException;
    void unsubscribe(String topic);
    <T> void publish(String topic, String message);

    boolean acquire_group_lock(String group, String sender);

    String responseTopic(String sender);
    String healthTopic(String ident);
    void registerIdent(UUID uuid);
    void unRegisterIdent(UUID uuid);

    void reportError(GilmourProtocol.GilmourErrorResponse message);

    void start(GilmourRequestExecutor e, Logger l);

    void stop();
}
