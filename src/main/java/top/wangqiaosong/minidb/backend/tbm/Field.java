package top.wangqiaosong.minidb.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import top.wangqiaosong.minidb.backend.im.BPlusTree;
import top.wangqiaosong.minidb.backend.parser.statement.SingleExpression;
import top.wangqiaosong.minidb.backend.tm.TransactionManagerImpl;
import top.wangqiaosong.minidb.backend.utils.Panic;
import top.wangqiaosong.minidb.backend.utils.ParseStringRes;
import top.wangqiaosong.minidb.backend.utils.Parser;
import top.wangqiaosong.minidb.common.Error;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 * 通过vm的entry存储 格式为dataItem
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;

    /**
     * 读取字段
     * 根据这个结构，通过一个 UID 从表中读取字段并解析如下：
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            //读取一个entry.data()
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 读取并解析字段
     * 通过vm，但是xmin和xmax都是8字节，要跳过才能拿到data，从entry中data字段拿数据
     * vm的具体数据又通过dm管理，所以真正想梳理清楚字节范围，需看三部分
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        //解析出字段名
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        //解析出字段类型
        fieldType = res.str;
        position += res.next;
        //该字段根节点的索引
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if (index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 创建字段
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        //有索引
        if (indexed) {
            //生成一个空的根节点   ，返回存放根节点的uid
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            //以该uid为bootUid的节点为根节点建立一个B+树
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    /**
     * 创建一个字段的方法类似，将相关的信息通过 VM 持久化即可
     * 把field的value持久化 fieldName fieldType index
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        //将field包装为entry交给dm插入
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    /**
     * B+树范围查询
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) v);
                break;
            case "int64":
                raw = Parser.long2Byte((long) v);
                break;
            case "string":
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                //根据类型复制raw数组不同的长度
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int) v);
                break;
            case "int64":
                str = String.valueOf((long) v);
                break;
            case "string":
                str = (String) v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }

    /**
     * a>5  left=6 right=MAX_VALUE
     * a<5  left=0 right=4;
     * a=5  left=right=5
     * 找出表的数据区间
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if (res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
