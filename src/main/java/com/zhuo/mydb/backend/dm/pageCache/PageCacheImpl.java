package com.zhuo.mydb.backend.dm.pageCache;

import com.zhuo.mydb.backend.common.AbstractCache;
import com.zhuo.mydb.backend.dm.page.Page;
import com.zhuo.mydb.backend.dm.page.PageImpl;
import com.zhuo.mydb.backend.utils.Panic;
import com.zhuo.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : wzq
 * @since : 2024-10-23 20:50
 **/
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;
    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno ,initData ,null);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    private void flush(Page pg){
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno ,buf.array() ,this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        }catch (IOException e){
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }
}
