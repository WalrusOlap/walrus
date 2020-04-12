package org.pcg.walrus.common.exception;

public class WMetaException extends WException {

    private static final long serialVersionUID = 5544888127914323916L;

    public WMetaException(String msg) {
        super("[WALRUS_META]" + msg, 0);
        this.msg = msg;
    }

    public WMetaException(String msg, int errorCode) {
        super("[WALRUS_META]" + msg, errorCode);
        this.msg = msg;
    }

}
