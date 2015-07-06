package gilmour;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

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

        static GilmourData createGilmourData() {
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
        private final String message;
        private final String stacktrace;

        public GilmourErrorResponse(int code, String sender, String topic, String request_data, String message, String stacktrace) {
            this.code = code;
            this.request_data = request_data;
            this.message = message;
            this.stacktrace = stacktrace;
            this.topic = topic;
            this.sender = sender;
        }

        public int getCode() {
            return code;
        }

        public String toString() {
            return message + "\n" + stacktrace;
        }

        public String getSender() {
            return sender;
        }

        public String getTopic() {
            return topic;
        }

        public String getRequest_data() {
            return request_data;
        }

        public String getMessage() {
            return message;
        }

        public String getStacktrace() {
            return stacktrace;
        }
    }

    public static class GilmourEmptyResponse {}
}
