package com.aliyun.openservices.loghub.client;

import java.util.concurrent.Callable;


interface ITask extends Callable<TaskResult> {
	TaskResult call();
}
