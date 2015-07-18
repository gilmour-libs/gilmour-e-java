package gilmour;

/**
 * Created by aditya@datascale.io on 16/07/15.
 */
public class GilmourPublishOpts {
    private int timeout = 60000; // One minute
    private boolean confirmSubscribers = false;

    public GilmourPublishOpts(int timeout, boolean confirmSubscribers) {
        this.timeout = timeout;
        this.confirmSubscribers = confirmSubscribers;
    }

    private GilmourPublishOpts() {}

    public static GilmourPublishOpts createGilmourPublishOpts() {
        return new GilmourPublishOpts();
    }

    public int getTimeout() {
        return timeout;
    }

    public GilmourPublishOpts setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public boolean confirmSubscribers() {
        return confirmSubscribers;
    }

    public GilmourPublishOpts setConfirmSubscribers() {
        this.confirmSubscribers = true;
        return this;
    }
}
