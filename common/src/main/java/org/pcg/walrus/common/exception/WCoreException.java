package org.pcg.walrus.common.exception;

public class WCoreException extends WException {

    private static final long serialVersionUID = 5544888127914323916L;

    public WCoreException(String msg) {
        super("[WALRUS_CORE]" + msg, 0);
        this.msg = msg;
    }

    public WCoreException(String msg, int errorCode) {
        super("[WALRUS_CORE]" + msg, errorCode);
        this.msg = msg;
    }

}
