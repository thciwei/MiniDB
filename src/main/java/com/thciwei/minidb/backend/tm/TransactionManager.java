package com.thciwei.minidb.backend.tm;

import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事务管理接口
 */
public interface TransactionManager {
    /**
     * 基本操作
     *
     * @return
     */
    long begin();                       // 开启一个新事务

    void commit(long xid);              // 提交一个事务

    void abort(long xid);               // 终止一个事务

    /**
     * 检查事务的三种状态
     *
     * @param xid
     * @return
     */
    boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态

    boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交

    boolean isAborted(long xid);        // 查询一个事务的状态是否是已回滚

    void close();                       // 关闭TM

    /**
     * 创建一个以.xid为结尾的文件
     */
    public static TransactionManagerImpl create(String path) {

        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            //RandomAccessFile创造通道
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);

        try {
            //写一个xid空文件头，长度8字节
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);

    }

    public static TransactionManagerImpl open(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);

    }


}
