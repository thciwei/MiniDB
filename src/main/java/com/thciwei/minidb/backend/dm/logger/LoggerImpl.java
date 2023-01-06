package com.thciwei.minidb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.backend.utils.Parser;
import com.thciwei.minidb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc * 日志文件读写
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * <p>
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {
    /**
     * (数据完整性检查)校验和|种子
     */
    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4; //4
    private static final int OF_DATA = OF_CHECKSUM + 4; //8
    public static final String LOG_SUFFIX = ".log";
    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;


    /**
     * 创建日志文件构造器
     */
    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public void init() {
        long size = 0;

        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
        //Size 是一个四字节整数
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            //初始时从0读到4，故除init外其他方法都是从4开始读或写的
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除 badTail
     */
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }
        try {
            //截断正常日志排除坏日志
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 将数据包装，向日志文件中写入日志
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        //先写入日志再去改校验和
        updateXChecksum(log);
    }

    /**
     * 更新checksum
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            //刷新缓冲区，保证内容写入磁盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }


    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 数据先包装为日志格式
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        //拼接为我们定义的日志格式
        return Bytes.concat(size, checkSum, data);
    }

    /**
     * 截取文件前x字节
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } catch (IOException e) {
            lock.unlock();
        }
    }


    /**
     * logger设计为迭代器,next依赖着internNext
     */
    public byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        // 1读取size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            //读完position移动到4
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize) {
            return null;
        }
        //2读取checksum+data 4+4+4  从4开始
        ByteBuffer buffer = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = buffer.array();
        //3校验 checksum 比较校验和大小
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;

    }

    /**
     * 设计为迭代器
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void rewind() {
        position = 4;
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
