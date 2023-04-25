package top.wangqiaosong.minidb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.wangqiaosong.minidb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * <p>
 * 死锁应该指两个事务，也就是xid和uid都是事务id，与dm无关
 * 整体基于等待图，包括锁的信息和事务等待信息两部分
 */
public class LockTable {
    /**
     * XID已经获得的资源的UID列表   事务在操纵哪些资源
     */
    private Map<Long, List<Long>> x2u;
    /**
     * UID被某个XID持有
     */
    private Map<Long, Long> u2x;
    /**
     * 正在等待UID的XID列表
     */
    private Map<Long, List<Long>> wait;
    /**
     * 正在等待资源的XID的锁
     */
    private Map<Long, Lock> waitLock;
    /**
     * XID正在等待的UID
     */
    private Map<Long, Long> waitU;
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常

    /**
     * 出现等待则加边并验证是否加边产生环(死锁)
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            //isInList接收u0、u1，xid和uid都是事务？
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            waitU.put(xid, uid);
            putIntoList(wait, xid, uid);
            if (hasDeadLock()) {
                //死锁了就撤销事务，remove换下一个事务补进来
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从头开始解锁，让业务线程继续执行
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            //释放锁和事务
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }



    /**
     * 从等待队列中选择一个xid来占用uid
     * 循环释放掉了这个线程所有持有的资源的锁，这些资源可以被等待的线程所获取
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if (l == null) return;
        assert l.size() > 0;

        while (l.size() > 0) {
            long xid = l.remove(0);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if (l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    //访问戳
    private int stamp;

    /**
     * 查找图中是否有环的算法也非常简单，就是一个深搜，
     * 只是需要注意这个图不一定是连通图。思路就是为每个节点设置一个访问戳，都初始化为 1，
     * 随后遍历所有节点，以每个非 1 的节点作为根进行深搜，并将深搜该连通图中遇到的所有节点都设置为同一个数字，
     * 不同的连通图数字不同。这样，如果在遍历某个图时，遇到了之前遍历过的节点，说明出现了环。
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        //每个xid对应着一些和uid连成的边,与xid连接的那些边
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        //stp==stamp发现和原来已经访问的边说明出现环了，死锁
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        //如果uid=null说明没有等待，!=null说明正在等待，产生死锁了
        if (uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    /**
     * 如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) return;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                i.remove();
                break;
            }
        }
        if (l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        //等于list集合在0处添加了uid，每次putintolist都是新建一个list但只用0这一处
        listMap.get(uid0).add(0, uid1);
    }


    /**
     * 两个事务是否有边
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        //事务0拿到事务0指向的边集合
        List<Long> l = listMap.get(uid0);
        if (l == null) return false;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            //该集合中有id与事务1 id相等则事务0和1有边
            long e = i.next();
            if (e == uid1) {
                return true;
            }
        }
        return false;
    }

}
