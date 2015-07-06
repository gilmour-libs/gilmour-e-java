package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */
public class GilmourHandlerOpts {

    private String group = null;
    private boolean oneshot = false;
    private boolean sendResponse = true;

    public boolean sendResponse() {
        return sendResponse;
    }

    public GilmourHandlerOpts setSendResponse(boolean sendResponse) {
        this.sendResponse = sendResponse;
        return this;
    }

    private GilmourHandlerOpts() {
    }

    public static GilmourHandlerOpts createGilmourHandlerOpts() {
        return new GilmourHandlerOpts();
    }

    public String getGroup() {
        return group;
    }

    public GilmourHandlerOpts setGroup(String group) {
        this.group = group;
        return this;
    }

    public boolean isOneshot() {
        return oneshot;
    }

    public GilmourHandlerOpts setOneshot() {
        this.oneshot = true;
        return this;
    }
}
