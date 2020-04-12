package org.pcg.walrus.common.exception;

import org.apache.commons.lang3.StringUtils;

public class WException extends Exception {

    private static final long serialVersionUID = 1L;

    public static int SYSTEM_ERROR = 101; // unknown system error
    public static int ERROR_DAYS_NOT_COVER = 102; // table or view unsupported days
    public static int ERROR_DATA_NOT_RAEDY = 103; // data nor ready
    public static int ERROR_COLUMN_NOT_COVER = 104; // field not cover

    protected String msg;
    protected int errorCode = 0;

    public WException(String msg, int errorCode) {
        super(msg);
        this.errorCode = errorCode;
    }

    /**
     *
     */
    public int getErrorCode() {
        if(errorCode != 0 ) return errorCode;
        if(StringUtils.isBlank(msg)) return SYSTEM_ERROR;
        if(msg.contains("unsupported")) return ERROR_DAYS_NOT_COVER;
        if(msg.contains("not ready")) return ERROR_DATA_NOT_RAEDY;
        if(msg.contains("unknown columns:")) return ERROR_COLUMN_NOT_COVER;
        if(msg.contains("unknown metrics:")) return ERROR_COLUMN_NOT_COVER;
        if(msg.contains("Unknown view or table:")) return ERROR_COLUMN_NOT_COVER;
        return SYSTEM_ERROR;
    }

    public String getErrorMsg() {
        return msg;
    }
}
