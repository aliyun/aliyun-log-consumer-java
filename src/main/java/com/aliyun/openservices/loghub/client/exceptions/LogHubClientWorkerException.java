package com.aliyun.openservices.loghub.client.exceptions;

public class LogHubClientWorkerException extends Exception{


	private static final long serialVersionUID = 5677182518574807776L;
	
	
	public LogHubClientWorkerException(String message) {
		super(message);
	}

	public LogHubClientWorkerException(String message, Throwable cause) {
		super(message, cause);
	}

}
