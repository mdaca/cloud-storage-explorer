package com.spinsys.mdaca.storage.explorer.model.exception;

import java.io.IOException;

public class ExplorerException extends IOException {

	private static final long serialVersionUID = 1L;

	private boolean useMessage = false;

	public ExplorerException(String message) {
		super(message);
	}

	public ExplorerException(String message, boolean useMessage) {
		super(message);

		this.useMessage = useMessage;
	}

	public ExplorerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ExplorerException(Throwable cause) {
		this(cause.getMessage(), cause);
	}

	public boolean isUseMessage() {
		return useMessage;
	}

	public void setUseMessage(boolean useMessage) {
		this.useMessage = useMessage;
	}

}
