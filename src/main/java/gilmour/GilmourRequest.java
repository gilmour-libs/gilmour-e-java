package gilmour;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public interface GilmourRequest {
    String sender();
    <T> T data(Class<T> cls);
    <T> T data(Type type);
    String stringData();

    String topic();
    int code();
}
