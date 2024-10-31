package com.zhuo.mydb.backend.dm;

import com.zhuo.mydb.backend.common.AbstractCache;
import com.zhuo.mydb.backend.dm.dataItem.DataItem;
import com.zhuo.mydb.backend.dm.dataItem.DataItemImpl;
import com.zhuo.mydb.backend.dm.logger.Logger;
import com.zhuo.mydb.backend.dm.page.Page;
import com.zhuo.mydb.backend.dm.page.PageOne;
import com.zhuo.mydb.backend.dm.page.PageX;
import com.zhuo.mydb.backend.dm.pageCache.PageCache;
import com.zhuo.mydb.backend.dm.pageIndex.PageIndex;
import com.zhuo.mydb.backend.dm.pageIndex.PageInfo;
import com.zhuo.mydb.backend.tm.TransactionManager;
import com.zhuo.mydb.backend.utils.Panic;
import com.zhuo.mydb.backend.utils.Types;
import com.zhuo.mydb.common.Error;

/**
 * @author : wzq
 **/
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }


    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //构造一条记录
        byte[] raw = DataItem.wrapDataItemRaw(data);
        //如果当前记录 大于 一页的记录 则直接抛出错误不存储
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        //重复尝试5次，不加锁的原因：插入是高频操作，加锁影响性能
        for (int i = 0; i < 5; i++) {
            //找到相应的存储位置
            pi = pIndex.select(raw.length);
            if (pi != null) {
                //找到了，出循环
                break;
            } else {
                //没找到符合位置，申请新页
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        //尝试5次不行，抛出数据库繁忙
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            //通过页号获取实际的页面
            pg = pc.getPage(pi.pgno);
            //生成一份插入的日志，用于数据恢复，相当于redoLog
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            //物理插入
            short offset = PageX.insert(pg, raw);
            //将页面缓存释放，解决一致性问题
            pg.release();
            //通过页号和偏移量获得唯一标识并返回
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将页剩下的空间放到缓存中，方便下次快速找到空闲的位置
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
