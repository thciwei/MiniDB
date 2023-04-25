package top.wangqiaosong.minidb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.wangqiaosong.minidb.backend.common.SubArray;
import top.wangqiaosong.minidb.backend.dm.DataManager;
import top.wangqiaosong.minidb.backend.dm.dataItem.DataItem;
import top.wangqiaosong.minidb.backend.im.Node.InsertAndSplitRes;
import top.wangqiaosong.minidb.backend.im.Node.LeafSearchRangeRes;
import top.wangqiaosong.minidb.backend.im.Node.SearchNextRes;
import top.wangqiaosong.minidb.backend.tm.TransactionManagerImpl;
import top.wangqiaosong.minidb.backend.utils.Parser;

/**
 * @author * 每棵B+树都有一个bootUUID, 可通过它向DM读取该树的boot.
 * B+树boot里面存储了B+树根节点的地址.
 * PS: 因为B+树在算法执行过程中, 根节点可能会发生改变, 所以不能直接用根节点的地址当boot,
 * 而需要一个固定的boot, 用来指向它的根节点.
 */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    /**
     * CreateBPlusTree 创建一棵B+树, 并返回其bootUUID.
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        //B+ 树在插入删除时，会动态调整，根节点不是固定节点，
        // 于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。
        // 可以注意到，IM 在操作 DM 时，使用的事务都是 SUPER_XID。提高优先级不受其他影响
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }


    /**
     * 通过BootUUID读取一课B+树, 并返回它.
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * rootUUID 通过bootUUID读取该树的根节点地址
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新该树的根节点
     * left 原来的根节点uid
     * right，新的分裂出来的节点的uid
     * rightKey新的分裂出来的节点的第一个索引
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            //生成一个根节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            //替换uid为新的uid
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 根据key, 在nodeUID代表节点的子树中搜索, 直到找到其对应的叶节点地址.
     * 从uid为nodeUid的节点开始寻找索引为key的数据的uid(直到找到叶子节点)
     */

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            //叶子节点
            return nodeUid;
        } else {
            //找到索引为key的uid，继续往下搜索，直到搜到叶子节点
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 从nodeUID对应节点开始, 不断的向右试探兄弟节点, 找到对应key的next uid
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 与Node节点类似，不过要先一直定位到对应的叶子节点的位置，再按照Node节点的查询方法进行查询
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        //从uid为rootUid的节点开始寻找索引为leftKey的数据的叶子节点的uid
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            // 不断的从leaf向sibling迭代, 将所有满足的uuid都加入
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 向B+树中插入(uid, key)的键值对
     * 从根节点开始查找，插入一个新节点
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        //原节点已满，分裂出一个新的节点，则生成一个根节点，根节点的key保存原节点和新节点的key|uid。
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 将(uid, key)插入到B+树中, 如果有分裂, 则将分裂产生的新节点也返回.
     * 从nodeUid节点开始找到要插入的位置插入
     * 如果nodeUid是叶子节点，直接插入
     * 如果nodeUid不是叶子节点，在下一层找到叶子节点再插入
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            //去下一层找叶子结点插入
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 从node开始, 不断的向右试探兄弟节点, 直到找到一个节点, 能够插入进对应的值
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
