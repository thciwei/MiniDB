package com.thciwei.minidb.backend.tm;

import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.backend.utils.Parser;
import com.thciwei.minidb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 */

public class TranscationManagerImlp implements TranscationManager {

    /**
     * XID是对事务的一个唯一标识(名字任意),从1开始,之后数组内存寻址也是如此
     * 可以结合接口的create来看
     */
    //XID文件 头的长度为8
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务占用1个字节
    private static final int XID_FIELD_SIZE = 1;
    static final String XID_SUFFIX = ".XID";
    private long XID;

    /**
     * 事务的三种状态
     */
    //开启
    private static final byte FIELD_TRAN_ACTIVE = 0;
    //已提交
    private static final byte FIELD_TRAN_COMMITTED = 1;
    //回滚
    private static final byte FIELD_TRAN_ABORTED = 2;
    /**
     * 超级事务，判断时优先级最高，永远是commited状态,没有申请事务时XID为0
     */
    private static final long SUPER_XID = 0;


    private Lock counterLock;

    //RandomAccessFile用于读写文件
    private RandomAccessFile file;
    //NIO 通道，数据传输
    private FileChannel fc;

    TranscationManagerImlp(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXID();
    }

    /**
     * 检查XID文件是否合法
     */
    private void checkXID() {
        long fileLength = 0;
        try {
            fileLength = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        //小于8字节不合法
        if (fileLength < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
        ByteBuffer buffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            //指针指0，从0处开始读文件
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.XID = Parser.parseLong(buffer.array());
        //终止位置
        long endFile = getXIDPosition(this.XID + 1);

        if (endFile != fileLength) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private long getXIDPosition(long XID) {
        //数组寻址公式a[i]_address = base_address + i * data_type_size,从1开始使用 (i-1) * data_type_size
        return LEN_XID_HEADER_LENGTH + (XID - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新XID事务状态status
     */
    private void updateXID(long XID, byte status) {
        long offset = getXIDPosition(XID);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tmp);

        try {
            fc.position(offset);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //执行文件操作后将通道数据立即刷入文件！防丢失！元数据不变
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * XID自增 1，更新XID文件头Header
     */
    private void increaseXID() {
        XID++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(XID));
        try {
            fc.position(0);
            fc.write(buffer);
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
     * 实现接口方法
     */

    /**
     * 开启一个事务，返回XID
     * 开启事务自动加锁，隔离
     */
    public long begin() {
        try {
            counterLock.lock();
            long xid = XID + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            increaseXID();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }


    @Override
    public void commit(long XID) {
        updateXID(XID, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long XID) {
        updateXID(XID, FIELD_TRAN_ABORTED);
    }

    /**
     * 检查XID事务当前是否处于status状态
     *
     * @param XID
     * @param status
     */
    private boolean checkXIDStatus(long XID, byte status) {
        long offset = getXIDPosition(XID);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //第一个字节表示当前状态
        return buf.array()[0] == status;
    }


    public boolean isActive(long XID) {
        //超级xid，最高级别不用检查状态
        if (XID == SUPER_XID) {
            return false;
        }
        return checkXIDStatus(XID, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long XID) {
        if (XID == SUPER_XID) {
            return true;
        }
        return checkXIDStatus(XID, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long XID) {
        if (XID == SUPER_XID) {
            return false;
        }
        return checkXIDStatus(XID, FIELD_TRAN_ABORTED);
    }


    @Override
    public void close() {

        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
