package top.wangqiaosong.minidb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.wangqiaosong.minidb.backend.dm.pageCache.PageCache;

/**
 * pindex 实现了对(Pgno, FreeSpace)键值对的缓存.
 * 其中FreeSpace表示的是Pgno这一页还剩多少空间可用.
 * pindex存在目的在于, 当DM执行Insert操作时, 可用根据数据大小, 快速的选出有适合空间的页.
 * 目前pindex的算法非常简单.
 * 设置 threshold := pcacher.PAGE_SIZE / _NO_INTERVALS,
 * 然后划分出_NO_INTERVALS端区间, 分别表示FreeSpace大小为:
 * [0, threshold), [threshold, 2*threshold), ...
 * 每个区间内的页用链表组织起来.
 * <p>
 * dm层是要由上层模块调用的，其中包含了对并发操作的一些规定，导致下层方法的一些特殊设计逻辑
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    /**
     * 按40一个划分后的表空间阈值
     * 每个空间的大小
     */
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    /**
     * 索引,结构：一个索引号对应一个pageInfo
     */
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * select移除了页面
     * 上层模块对此页使用完再添加回索引
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 被选择的页面会被移除，不支持页面的并发写入
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            //空间数量，刚好40是直接移除第40号区间数据，不是40号索引就+1只要数据不空就也移除了
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
