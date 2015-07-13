package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public class GilmourResponder {
    private final String senderchannel;
    private Object message = null;
    private int code = 0;

    public boolean isResponseSent() {
        return responseSent;
    }

    private boolean responseSent = false;

    public GilmourResponder(String sender) {
        this.senderchannel = sender;
    }

    public <T> void respond(T response) {
        message = response;
    }

    public <T> void respond(T response, int code) {
        message = response;
        this.code = code;
    }

    public void send(Gilmour gilmourinst) {
        if (responseSent) return;
        if (code == 0)
            gilmourinst.publish(senderchannel, message);
        else
            gilmourinst.publish(senderchannel, message, code);
        responseSent = true;
    }
}
