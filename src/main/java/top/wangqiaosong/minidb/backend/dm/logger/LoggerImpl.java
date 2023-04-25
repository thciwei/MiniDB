package top.wangqiaosong.minidb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.wangqiaosong.minidb.backend.utils.Panic;
import top.wangqiaosong.minidb.backend.utils.Parser;
import top.wangqiaosong.minidb.common.Error;

/**
 * 日志文件读写
 * <p>
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * <p>
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 * raw在oracle可以理解为一些二进制信息数据类型，这里可以理解为底层的一些信息
 */
public class LoggerImpl implements Logger {
    //校验和是冗余校验的一种形式。
    // 它是通过错误检测方法，对所传送数据的完整性进行检查的一种简单方法。
    //校验种子:常用于CRC校验
    //这里用的可能是哈希、累加结合的简化版本
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
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
            /**
             * 初始时从0读到4，故除init外其他方法都是从4开始读或写的
             */
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

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        //init时日志只有XChecksum（所以截断4之前的数据） 有可能有没写完的BadTail，这一节和事务有关联
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
            //之前的一些操作会移动position，此时position之前的值便是BadTail之前的值
            // 截断文件到正常日志的末尾
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private int calChecksum(int xCheck, byte[] log) {
        //Java中涉及byte、short和char类型的运算操作首先会把这些值转换为int类型
        //(byte=2 )+(int=1) =3
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 重新讲讲通道和buffer读写文件，
     * 首先将数据wrap包装成buffer，然后通过通道直接读写，其中需要position指定写的位置(从哪里开始写入),
     * 向日志文件中写日志
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

    /**
     * 将数据先包装成日志格式
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        //三段拼成一个字节数组
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            //截取文件的前x字节
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    //日志格式[Size][Checksum][Data] Size默认为4 Checksum=4 这段文件从size开始读
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        // 1读取size
        ByteBuffer tmp = ByteBuffer.allocate(4);//filechannel写入数据
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize) {
            return null;
        }
        /** 2读取checksum+data 4+4+4，其实都是写死的大小，size为4.单个日志checksum为4，data为size大小也是4，
         * 写成变量只是为了便于修改和扩展
         * 真正要求的是总的校验和
         **/
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        // 3校验 checksum 比较校验和大小  8->12
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        //4->8
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

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
