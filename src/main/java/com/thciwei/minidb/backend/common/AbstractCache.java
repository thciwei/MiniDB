package com.thciwei.minidb.backend.common;

import com.thciwei.minidb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个使用引用计数策略的缓存
 * 其他的缓存只需要继承这个类，并实现 releaseForCache和 getForCache两个抽象方法
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 */
public abstract class AbstractCache<T> {
    /**
     * 应对多线程，cache references getting 用于记录计数过程中的其它数据
     */

    /**
     * 实际缓存
     */
    private HashMap<Long, T> cache;
    /**
     * 资源的引用个数，也就是我们的引用计数
     */
    private HashMap<Long, Integer> references;
    /**
     * 正在被获取的资源
     */
    private HashMap<Long, Boolean> getting;
    /**
     * 缓存上限
     */
    private int maxResource;
    /**
     * 缓存元素个数
     */
    private int count = 0;
    private Lock lock;

    /**
     * 缓存构造器
     * @param maxResource
     */
    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为 (缓存清除)
     */
    protected abstract void releaseForCache(T obj);

    /**
     * 不断获取资源
     * @param key
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            /**
             * key已经存在说明其它线程正在获取，等待一段时间，continue跳出循环开始下一次的请求资源
             * 如果key不存在，getting.containsKey(key)为false直接进入后面代码获取内存中的资源返回即可
             * 每次获取了资源删除key
             */
            if (getting.containsKey(key)) {
                //请求的资源正在被其它线程获取时，解锁
                lock.unlock();
                try {
                    //等待一段时间再去获取资源
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            if (cache.containsKey(key)) {
                //资源在缓存中,直接返回，资源的引用数 +1
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                return obj;
            }
            //尝试获取该资源，但缓存满了抛异常
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;

        }
        T obj = null;
        try {
            //真正的去获取资源
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            //获取资源后删除key
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //一个用户获取了资源
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        try {
            lock.lock();
            int ref = references.get(key) - 1;
            //如果计数值为0直接释放回源，删除缓存结构
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 安全问题，缓存关闭时回收所有缓存中的资源
     */
    protected void close() {
        lock.lock();
        try {
            //拿到key集合
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


}
