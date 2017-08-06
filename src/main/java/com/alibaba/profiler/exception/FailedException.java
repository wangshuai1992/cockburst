package com.alibaba.profiler.exception;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-9
 * Time: обнГ5:30
 * To change this template use File | Settings | File Templates.
 */
public class FailedException extends Exception {

    public FailedException() {
        super();
    }

    public FailedException(final String message) {
        super(message);
    }

    public FailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public FailedException(final Throwable cause) {
        super(cause);
    }
}
