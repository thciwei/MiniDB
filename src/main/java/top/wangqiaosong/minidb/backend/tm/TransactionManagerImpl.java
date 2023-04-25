package top.wangqiaosong.minidb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.wangqiaosong.minidb.backend.utils.Panic;
import top.wangqiaosong.minidb.backend.utils.Parser;
import top.wangqiaosong.minidb.common.Error;

/**
 * 使用了NIO对数据进行读写
 * MySQL中redo log 和 binlog 相配合的时候，它们有一个共同的字段叫作 Xid。它在 MySQL 中是用来对应事务的。
 * 每个事务有一个自己的XID，数据库要通过XID才能查询事务和对某个事务进行相关操作，多个事务的信息都存储在一个.xid文件中，
 * 该文件的头部为事务的数量占8字节，随后是各个事务的状态
 */
public class TransactionManagerImpl implements TransactionManager {

    // XID是一个文件，它的头部有一个长度为8的数字，从1开始取记录事务数量，超级事务时为0
    //XID:[transaction numbers|trans status...more...]  这个文件头在程序中叫做xid，xid后面紧跟事务状态
    //其实我们可以把任意一种数据转为缓冲区，如[1,2,3,4]转为buf也是ok的
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    //开启事务
    private static final byte FIELD_TRAN_ACTIVE = 0;
    //已提交
    private static final byte FIELD_TRAN_COMMITTED = 1;
    //回滚
    private static final byte FIELD_TRAN_ABORTED = 2;

    /**
     * 超级事务，永远为commited状态
     */
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";
    //NIO常用
    private RandomAccessFile file;
    //NIO 通道，文件传输
    private FileChannel fc;
    /**
     * xidCounter就是xid,是long类型配合buffer
     */
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        //this的作用是调用本类(TransactionManagerImpl)中的属性，也就是类中的成员变量
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            //nio读文件拿到的长度是long类型
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        //小于8字节不合法
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
        //申请一个8字节的缓冲区
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            //position一开始都是0,相当于指针,每次put，指针向后一位
            fc.position(0);
            //通道数据读到缓冲区中
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //array是字节数组，当前位置由byte[]->buf转为long类型，xidCounter=几个Long
        this.xidCounter = Parser.parseLong(buf.array());
        //寻址：从8开始 8+(n-1)*1
        //TODO 1.为什么加1，因为是寻址公式是xid-1，如果不加1，那end=8，但是end应为8+1*XID_FIELD_SIZE，数组长16包含了事务长度和事务状态
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }


    /**
     * 根据事务xid取得其在xid文件中对应的位置,数组是一组连续的内存空间.  此法拿到内存地址
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        //数组寻址公式，从1开始版本 a[i]_address = base_address + i * data_type_size,地址为8+(xid-1)*大小
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        //根据xid获取当前内存中位置，然后在当前位置重写status(必须包装为buffer才能写入)
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header,说明增加了新事务
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //force方法:文件操作执行后,channel中的数据立刻刷入文件中，强制同步缓存内容到文件，元数据不用动(false)
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个事务，并返回XID，版本管理时会使用该方法
     * @return
     */
    public long begin() {
        //数据库，开启事务就自动加锁，锁是解决并发、隔离性的一种机制
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //第一个字节用来表示状态
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        //超级xid，不用检查状态了
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
