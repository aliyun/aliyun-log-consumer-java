package com.aliyun.openservices.loghub.client.throttle;

public interface ResourceBarrier {

    void acquire(long permits);

    boolean tryAcquire(long permits);

    void release(long permits);
}
