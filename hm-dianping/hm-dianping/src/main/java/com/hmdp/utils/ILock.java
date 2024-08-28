package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSecond
     * @return
     */
    boolean tryLock(long timeoutSecond);

    /**
     * 释放锁
     */
    void unlock();
}
