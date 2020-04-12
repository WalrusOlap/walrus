package org.pcg.walrus.common.exception;

public class WServerException extends WException {

    private static final long serialVersionUID = 5544888127914323916L;

    public WServerException(String msg) {
        super("[WALRUS_SERVER]" + msg, 0);
        this.msg = msg;
    }

    public WServerException(String msg, int errorCode) {
        super("[WALRUS_SERVER]" + msg, errorCode);
        this.msg = msg;
    }

}
