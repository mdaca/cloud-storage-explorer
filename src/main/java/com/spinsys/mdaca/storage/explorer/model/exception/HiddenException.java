package com.spinsys.mdaca.storage.explorer.model.exception;

public class HiddenException extends ExplorerException {

    public HiddenException(String message) {
        super(message, false);
    }

}
