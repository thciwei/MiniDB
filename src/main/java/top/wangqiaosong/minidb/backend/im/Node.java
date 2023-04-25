package top.wangqiaosong.minidb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import top.wangqiaosong.minidb.backend.common.SubArray;
import top.wangqiaosong.minidb.backend.dm.dataItem.DataItem;
import top.wangqiaosong.minidb.backend.tm.TransactionManagerImpl;
import top.wangqiaosong.minidb.backend.utils.Parser;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * 1          2           8
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 8      8     8
 * 在一般的B+树算法中, 内部节点都会有一个MaxPointer, 指向最右边的子节点.
 * 我们这里将其特殊处理, 将MaxPointer处理成了SonN, 最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
 * 这样, 内部节点和叶节点就有了一致的二进制结构.
 * 用词：sibling->兄弟 kth->代表第几个  nil->空  nokeys->该节点下子节点个数
 * 所有方法raw可以忽略只是为了转为二进制结构
 * <p>
 * 其中 LeafFlag 标记了该节点是否是个叶子节点；KeyNumber 为该节点中 key 的个数；
 * SiblingUid 是其兄弟节点存储在 DM 中的 UID。
 * 后续是穿插的子节点（SonN）和 KeyN。
 * 最后的一个 KeyN 始终为 MAX_VALUE
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
    /**
     * 当BALANCE_NUMBER*2=一个Node所含子节点的个数时，会出现节点分裂,对应着needSplit方法
     */
    static final int BALANCE_NUMBER = 32;
    /**
     * 一个Node的大小 一个key,Son都占8个字节=2*8
     */
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 设置叶子结点
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        //nokeys字段用2字节存储
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 把兄弟结点转为二进制结构
     */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        // node -> [ [16] [32]  [新节点] [] [] [] [][end] ]  从左向右移动，给新的元素提供位置
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    /**
     * 新建一个根节点,该根节点的初始两个子节点为left和right, 初始键值为key
     */
    static byte[] newRootRaw(long left, long right, long key) {
        //keyNumber可以不同，每个node大小相同
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //初始两个子节点为left和right，初始键值为key
        setRawIsLeaf(raw, false);//该结点不是叶子结点
        setRawNoKeys(raw, 2);//该节点有2个子节点
        setRawSibling(raw, 0);//根节点无邻节点
        //left||key ||right||Long.MAX_VALUE
        setRawKthSon(raw, left, 0); //left为第0个子节点的uid
        setRawKthKey(raw, key, 0);//key值
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * newNilRootRaw 新建一个空的根节点, 返回其二进制内容.
     */
    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     *  读入一个节点, 其自身地址为 selfuid
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        /**
         * 邻接点uid
         */
        long siblingUid;
    }

    /**
     * 寻找对应key的uid, 如果找不到, 则返回sibling uid
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 范围搜索
     */
    class LeafSearchRangeRes {
        List<Long> uids;//如果命中，返回范围内所有的uid
        long siblingUid;//如果没有命中，返回下一个邻节点
    }

    /**
     * LeafSearchRange 在该节点上查询属于[leftKey, rightKey]的地址,
     * 约定 如果rightKey大于等于该节点的最大的key, 则还返回一个sibling uuid.
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);//该节点有多少个子节点
            int kth = 0;
            while (kth < noKeys) {
                //第k+1个子节点的key
                long ik = getRawKthKey(raw, kth);
                //找到了满足范围的第一个kth
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                    //没有满足范围的索引
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            //该节点搜索完毕，则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }


    /**
     * 判断分裂方法
     * 每当插入一个节点后，都会检查是否需要分裂节点。
     * 1 如果插入不成功，把邻节点的uid赋值给siblingUid；
     * 2 如果插入成功，但是需要分裂，把新分裂出来的节点的uid和索引赋值给newSon, newKey；
     * 3 如果插入成功，不需要分裂，不做处理
     */
    //如果分裂，返回分裂出的节点的信息
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                //新插入的节点在当前节点的最后，且当前节点已经有邻节点
                //插入不成功，返回邻节点
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            //在插入新节点后raw节点已满的情况下，无法继续插入，
            // 生成一个邻节点插入到raw和raw的邻节点之间，且邻节点会分担一半的数据
            //返回存储邻节点的uid和开头索引
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 如果失败, 则复原当前节点
                dataItem.unBefore();
            }
        }
    }

    /**
     * 此方法只是认定可以插入为分裂做数据预处理，真正的分裂操作在 insertAndSplit
     * if和else逻辑很重要
     * 如果插入的元素已经在节点集合中那就移动一些给它一个位置，不分裂
     * 如果插入元素集合中没有，需要分裂
     * 举个例子如果是按 1,2,5，7,8,9的顺序插入的话最终结果
     *      [7   8]
     * [1 2] [5 7] [8 9]
     */
    private boolean insert(long uid, long key) {
        //获得子节点数量
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        //应该插入到raw的kth位置
        //要插入节点要插在当前节点的最后位置，且当前节点已经有邻节点，返回插入失败，下一步在邻节点进行插入
        if (kth == noKeys && getRawSibling(raw) != 0) return false;
        //如果找到插入位置为kth，且该节点是叶子节点，在当前节点的kth位置插一个key|son
        if (getRawIfLeaf(raw)) {
            //从kth开始所有节点往右移动，新的节点插入到kth位置
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            //System.out.println("插入操作不是叶子节点");
            //思路如果找到插入位置为kth，但该节点不是叶子节点
            //kth位置的索引移到kth+1上，kth位置放入新插入节点的索引
            //kth+1位置的uid改成新插入节点的uid

            //例如 把8插入到节点上
            //   [7 MAX_VALUE]
            //[1 2] [5 7 9]
            //变成
            //   [7 MAX_VALUE]此时，7存放的是[1 2]的uid，MAX_VALUE存放的是[5 7]的uid
            //[1 2] [5 7] [8 9]
            //private boolean insert(long uid, long key)这里uid是[8 9]的uid，key是8
            //如果按照第一个if
            //[7 8 MAX_VALUE]
            //[1 2] [8 9] [5 7]
            //如果按照else
            //[7 8 MAX_VALUE]
            //[1 2] [5 7] [8 9]
            long kk = getRawKthKey(raw, kth); //kk=MAX_VALUE
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    /**
     * 当节点的子节点装满时，需要分裂一个邻节点出来
     * 是否需要分裂
     */
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }



    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂方法
     * 1 在插入新的子节点后，节点已满的情况下，无法继续插入，此时，生成一个邻节点nodeRaw插入到原节点raw和原节点raw的邻节点之间，
     * 2 把原节点raw后一半的数据拷贝到邻节点noderaw
     * 3 此时，raw节点nodeRaw节点各有原来一半的数据，将nodeRaw节点设置为raw节点的邻节点
     * 返回nodeRaw节点的uid和索引
     */
    private SplitRes split() throws Exception {
        //四步操作：装载nodeRaw
        //给新的node划分一块内存
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);//1
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));//2
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);//3
        setRawSibling(nodeRaw, getRawSibling(raw));//4
        //从BALANCE_NUMBER（复制后一半的数据）开始把raw复制到noderaw里面，
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        //插入nodeRaw的uid
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        //返回存储邻节点的uid和开头索引
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        //System.out.println(res.newKey);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
