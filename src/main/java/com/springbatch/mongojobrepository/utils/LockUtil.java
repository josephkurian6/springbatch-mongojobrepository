package com.springbatch.mongojobrepository.utils;

import java.util.concurrent.ConcurrentHashMap;

public class LockUtil {

    private LockUtil(){
        throw new IllegalStateException("Utility class");
    }

    private static final ConcurrentHashMap<Long, Long> lockJobMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Long, Long> lockStepMap = new ConcurrentHashMap<>();

    public static Object acquireJobLock(Long id) {
        return lockJobMap.computeIfAbsent(id, k -> id);
    }

    public static void releaseJobLock(Long id) {
        if(lockJobMap.containsKey(id)) {
            lockJobMap.remove(id);
        }
    }

    public static Object acquireStepLock(Long id) {
        return lockStepMap.computeIfAbsent(id, k -> id);
    }

    public static void releaseStepLock(Long id) {
        if(lockStepMap.containsKey(id)) {
            lockStepMap.remove(id);
        }
    }
}
