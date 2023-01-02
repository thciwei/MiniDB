package com.thciwei.minidb.backend.dm.pageCache;

import com.thciwei.minidb.backend.common.AbstractCache;
import com.thciwei.minidb.backend.dm.page.Page;
import com.thciwei.minidb.backend.dm.page.PageImpl;
import com.thciwei.minidb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 页面缓存的具体实现类,
 * 需要继承抽象缓存框架,并且实现 getForCache() 和 releaseForCache()
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    /**
     * 非阻塞模型，线程安全的Integer,记录了当前打开的数据库文件有多少页
     */
    private AtomicInteger pageNumbers;

    /**
     * 缓存构造器
     */
    public PageCacheImpl(int maxResource) {
        super(maxResource);
    }


    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();

        return  new PageImpl(pgno,buffer.array(),this);
    }

    private static long pageOffset(int pgno) {
        //页码从1开始
        return (pgno - 1) * PAGE_SIZE;
    }

    //脏页面立刻写回文件中
    @Override
    protected void releaseForCache(Page pg) {
       if(pg.isDirty()){
           flush(pg);
           pg.setDirty(false);
       }

    }

    private void flush(Page pg){
        int pgno=pg.getPageNumber();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            ByteBuffer buffer=ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    /**
     * 新建页面，页数+1
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno=pageNumbers.incrementAndGet();
        Page pg=new PageImpl(pgno,initData,null);
        //新建的页面需要立即写回
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public void flushPage(Page pg) {

    }


}
