package com.aliyun.openservices.loghub.client.exceptions;

public class LogHubCheckPointException extends Exception{

	private static final long serialVersionUID = 7272451967449061921L;

	public LogHubCheckPointException(String message) {
		super(message);
	}

	public LogHubCheckPointException(String message, Throwable cause) {
		super(message, cause);
	}
}
