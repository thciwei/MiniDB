package com.thciwei.minidb.backend.dm.pageCache;


import com.thciwei.minidb.backend.dm.page.Page;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    /**
     * 数据页大小8Kb
     */
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    /**
     * 线程安全的获得当前打开文件的页数
     */
    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);

        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }
    public  static PageCacheImpl open(String path,long memory){
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead()||!f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

}
