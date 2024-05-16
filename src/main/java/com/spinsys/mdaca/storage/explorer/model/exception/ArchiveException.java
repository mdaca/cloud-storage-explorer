package com.spinsys.mdaca.storage.explorer.model.exception;

/**
 * an error thrown when an archived Drive Item is attempted to be accessed
 */
public class ArchiveException extends DisplayableException {

    public ArchiveException(String message) {
        super(message);
    }

}
