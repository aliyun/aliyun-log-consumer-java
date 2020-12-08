package com.aliyun.openservices.loghub.client.throttle;

public class UnlimitedResourceBarrier implements ResourceBarrier {

    @Override
    public void acquire(long permits) {
    }

    @Override
    public boolean tryAcquire(long permits) {
        return true;
    }

    @Override
    public void release(long permits) {
    }
}
