package com.aliyun.openservices.loghub.client.excpetions;

public class LogHubLeaseException extends Exception {

	private static final long serialVersionUID = -8707112632143256929L;

	public LogHubLeaseException(String message) {
		super(message);
	}

	public LogHubLeaseException(String message, Throwable cause) {
		super(message, cause);
	}
}
