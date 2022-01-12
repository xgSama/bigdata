package org.json;

/**
 * JSONException
 *
 * @author : xgSama
 * @date : 2021/11/17 16:54:50
 */
public class JSONException extends RuntimeException {

    public JSONException(String s) {
        super(s);
    }

    public JSONException(Throwable cause) {
        super(cause);
    }

    public JSONException(String message, Throwable cause) {
        super(message, cause);
    }
}
