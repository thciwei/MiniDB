package top.wangqiaosong.minidb.backend.server;

import top.wangqiaosong.minidb.backend.parser.Parser;
import top.wangqiaosong.minidb.backend.parser.statement.Abort;
import top.wangqiaosong.minidb.backend.parser.statement.Begin;
import top.wangqiaosong.minidb.backend.parser.statement.Commit;
import top.wangqiaosong.minidb.backend.parser.statement.Create;
import top.wangqiaosong.minidb.backend.parser.statement.Delete;
import top.wangqiaosong.minidb.backend.parser.statement.Insert;
import top.wangqiaosong.minidb.backend.parser.statement.Select;
import top.wangqiaosong.minidb.backend.parser.statement.Show;
import top.wangqiaosong.minidb.backend.parser.statement.Update;
import top.wangqiaosong.minidb.backend.tbm.BeginRes;
import top.wangqiaosong.minidb.backend.tbm.TableManager;
import top.wangqiaosong.minidb.common.Error;

/**
 * 处理的核心是 Executor 类，Executor 调用 Parser 获取到对应语句的结构化信息对象，
 * 并根据对象的类型，调用 TBM 的不同方法进行处理。
 * <p>
 * tmp在项目中译为临时，stat代表数据库的状态即除与数据关联外的主字段 commit、select等等
 */
public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if (xid != 0) {
            //异常终止
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 不是事务标识符就是sql表示符号
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        //isInstance意思是Begin类是否能强转由Parser解析出的stat类型
        if (Begin.class.isInstance(stat)) {
            if (xid != 0) {
                //嵌套事务异常，因为存在stat不同类别
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (Commit.class.isInstance(stat)) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (Abort.class.isInstance(stat)) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            //不是begin commit  abort，可能是insert update等的情况
            return execute2(stat);
        }
    }

    //解析sql获取stat字段Show、Select等并通过tbm处理   DML、DDL
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        // 创建一个临时事务
        if (xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if (Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if (Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create) stat);
            } else if (Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select) stat);
            } else if (Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert) stat);
            } else if (Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete) stat);
            } else if (Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
