package gilmour;

import gilmour.protocol.GilmourProtocol;

import java.lang.reflect.Type;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public class GilmourRequest {
    protected final String topic;
    protected final GilmourProtocol.RecvGilmourData gData;

    public GilmourRequest(String topic, GilmourProtocol.RecvGilmourData gd) {
        this.topic = topic;
        this.gData = gd;
    }

    public String sender() {
        return gData.getSender();
    }

    public <T> T data(Class<T> cls) {
        return this.gData.getData(cls);
    }

    public <T> T data(Type cls) {
        return this.gData.getData(cls);
    }

    public String topic() {
        return topic;
    }

    public int code() {
        return gData.getCode();
    }

    public String stringData() {
        return gData.rawData();
    }
}
