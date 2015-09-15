package com.aliyun.openservices.loghub.client.config;

/**
 * This config is used to control which position the worker instance should use
 * to consume a shard logstream if no check point is found in data base
 * 
 */
public enum LogHubCursorPosition {
	BEGIN_CURSOR, END_CURSOR, SPECIAL_TIMER_CURSOR
}
