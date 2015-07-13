package gilmour.protocol;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.util.UUID.randomUUID;

/**
 * Created by aditya@datascale.io on 06/07/15.
 */
public class GilmourProtocol {
    public static class GilmourData {
        private String sender;
        private int code;
        private Object data;

        private GilmourData() {
        }

        public static GilmourData createGilmourData() {
            return new GilmourData();
        }

        public String getSender() {
            return sender;
        }

        public GilmourData setSender(String sender) {
            this.sender = sender;
            return this;
        }

        public int getCode() {
            return code;
        }

        public GilmourData setCode(int code) {
            this.code = code;
            return this;
        }

        public Object getData() {
            return data;
        }

        public GilmourData setData(Object data) {
            this.data = data;
            return this;
        }
        public String render() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }

    }

    public static class RecvGilmourData {
        private String sender;
        private int code;
        private JsonElement data;

        public RecvGilmourData() {}

        public <T> T getData(Class<T> cls) {
            Gson gson = new Gson();
            return gson.fromJson(data, cls);
        }

        public <T> T getData(Type t) {
            Gson gson = new Gson();
            return gson.fromJson(data, t);
        }

        public String rawData() {
            return data.toString();
        }

        public String getSender() {
            return sender;
        }

        public int getCode() {
            return code;
        }
    }

    public static RecvGilmourData parseJson(String data) {
        Gson gson = new Gson();
        return gson.fromJson(data, RecvGilmourData.class);
    }


    public static String makeSenderId() {
        return randomUUID().toString();
    }


    public static class GilmourErrorResponse {
        private final int code;
        private final String sender;
        private final String topic;
        private final String request_data;
        private final String userdata;
        private final String backtrace;
        private final String timestamp;

        public GilmourErrorResponse(int code, String sender, String topic, String request_data, String userdata, String backtrace) {
            this.code = code;
            this.request_data = request_data;
            this.userdata = userdata;
            this.backtrace = backtrace;
            this.topic = topic;
            this.sender = sender;
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            this.timestamp = sdf.format(new Date());
        }

        public int getCode() {
            return code;
        }

        public String toString() {
            return userdata + "\n" + backtrace;
        }

        public String getSender() {
            return sender;
        }

        public String getTopic() {
            return topic;
        }

        public String getRequestData() {
            return request_data;
        }

        public String getUserdata() {
            return userdata;
        }

        public String getBacktrace() {
            return backtrace;
        }
    }

    public static class GilmourEmptyResponse {}
}
