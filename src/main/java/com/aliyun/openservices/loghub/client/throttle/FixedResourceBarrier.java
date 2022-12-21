package com.aliyun.openservices.loghub.client.throttle;

public class FixedResourceBarrier implements ResourceBarrier {

    private Long left;

    public FixedResourceBarrier(long maxSize) {
        left = maxSize;
    }

    @Override
    public synchronized void acquire(long permits) {
        left -= permits;
    }

    @Override
    public synchronized boolean tryAcquire(long permits) {
        if (left < permits) {
            return false;
        }
        left -= permits;
        return true;
    }

    @Override
    public synchronized void release(long permits) {
        left += permits;
    }
}
