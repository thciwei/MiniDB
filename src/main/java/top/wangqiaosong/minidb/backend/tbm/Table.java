package top.wangqiaosong.minidb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import top.wangqiaosong.minidb.backend.parser.statement.Create;
import top.wangqiaosong.minidb.backend.parser.statement.Delete;
import top.wangqiaosong.minidb.backend.parser.statement.Insert;
import top.wangqiaosong.minidb.backend.parser.statement.Select;
import top.wangqiaosong.minidb.backend.parser.statement.Update;
import top.wangqiaosong.minidb.backend.parser.statement.Where;
import top.wangqiaosong.minidb.backend.tm.TransactionManagerImpl;
import top.wangqiaosong.minidb.backend.utils.Panic;
import top.wangqiaosong.minidb.backend.utils.ParseStringRes;
import top.wangqiaosong.minidb.backend.utils.Parser;
import top.wangqiaosong.minidb.common.Error;

/**
 * Table表示表信息
 * 二进制结构如下：
 * [TableName][NextTable]
 * 表名        下一张表的uid
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * 链表结构
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    /**
     * 读取表
     * 根据 UID 从 Entry 中读取表数据的过程和读取字段的过程类似。
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建表，用于插入新表时使用
     * nextUid原来第一张表的uid
     * xid插入新表的xid
     * 要插入新表的所有信息
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            //这里代表输入可能多条语句，所以要一整条一整条的赋值构造
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            //为新表的字段存储到dm中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 表的解析过程如下，由于uid的字节数是固定的（8个），于是无需保存字段的个数。
     * 1 解析表名
     * 2 解析下一个表的uid
     * 3 解析表中的所有字段，且创建字段，把所有的字段存储在一个fields中
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        //表名
        name = res.str;
        position += res.next;
        //下一张表的uid
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;
        //字段的uid
        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * table持久化
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        //uid代表dm单位的id，vm插入最后导入dm存储
        uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 删除计数
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 1 确认要更新的是哪张表
     * 2 找出命中字段中满足where条件的uid，解析出存储的一条数据（表中可能有多个字段，以键值对存储）
     * 3 找到要更新的字段fd
     * 4 删除原有的字段，实则是设置xmax
     * 5 更新一条数据，往DM插入更新后的值得到uid
     * 6 在所有indexed字段建立的B+树中插入新的的key|uid
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        //在字段中匹配字段名然后跳出循环
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            //读取一个entry
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;

            ((TableManagerImpl) tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);

            count++;

            for (Field field : fields) {
                if (field.isIndexed()) {
                    //b+树加入新node
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 读操作处理where字段以及后面的字段
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 计算 Where 条件的范围， 比如Delete和Select都需要计算 Where，
     * 最终就需要获取到条件范围内所有的 UID，这里只支持了带有索引的两个条件的查询
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        //or字段连起来是false，其他是true
        boolean single = false;
        Field fd = null;
        if (where == null) {
            //没有指定where范围
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            //指定了where范围
            for (Field field : fields) {
                //指定了是查找哪个字段
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            //calWhere处理逻辑和比较符号，取出数据的区间
            CalWhereRes res = calWhere(fd, where);
            //第一个条件的低水位和高水位
            l0 = res.l0;
            r0 = res.r0;
            //第二个条件的低水位和高水位
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            //or字段，增加后一个条件的uid
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        /**
         * 范围符号
         */
        long l0, r0, l1, r1;
        /**
         * 标记符，or为false and和其他为true
         */
        boolean single;
    }

    /**
     * 在字段中搜寻满足where条件的高低水位如下，如果是and字段，取两个条件的高低水位的并集。
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            //两个条件的高低水位取交集 左边最大 右边最小
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                //取left的最大值
                if (res.l1 > res.l0) res.l0 = res.l1;
                //取right的最小值
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    /**
     * 解析table后就能够输出类似使用Gson解析后的状态
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
