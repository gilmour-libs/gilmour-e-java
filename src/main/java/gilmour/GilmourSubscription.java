package gilmour;

/**
 * Created by aditya@datascale.io on 19/05/15.
 */

public class GilmourSubscription {
    private GilmourHandlerOpts opts;
    private GilmourHandler handler;

    public GilmourSubscription(GilmourHandler handler, GilmourHandlerOpts opts) {
        this.setHandler(handler);
        this.setOpts(opts);
    }
    public GilmourHandlerOpts getOpts() {
        return opts;
    }

    public void setOpts(GilmourHandlerOpts opts) {
        this.opts = opts;
    }

    public GilmourHandler getHandler() {
        return handler;
    }

    public void setHandler(GilmourHandler handler) {
        this.handler = handler;
    }
}
