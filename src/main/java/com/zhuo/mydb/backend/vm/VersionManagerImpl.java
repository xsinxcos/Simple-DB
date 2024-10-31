package com.zhuo.mydb.backend.vm;

import com.zhuo.mydb.backend.common.AbstractCache;
import com.zhuo.mydb.backend.dm.DataManager;
import com.zhuo.mydb.backend.tm.TransactionManager;
import com.zhuo.mydb.backend.tm.TransactionManagerImpl;
import com.zhuo.mydb.backend.utils.Panic;
import com.zhuo.mydb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : wzq
 **/
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        //加锁，防止中途插入活跃的事务
        lock.lock();
        //获取当前活跃的事务
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }

        Entry entry = null;
        try {
            //通过 唯一标识 获取 Cache 中的数据
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException){
                return null;
            }else {
                throw e;
            }
        }
        //结合隔离级别判断该数据是否可见
        try {
            if(Visibility.isVisible(tm, t, entry)){
                return entry.data();
            }else {
                return null;
            }
        }finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        //构造一条记录
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        //将记录进行插入
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        //获取记录
        Entry entry = null;
        try {
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException){
                return false;
            }else {
                throw e;
            }
        }
        try {
            //可见性分析
            if(!Visibility.isVisible(tm, t, entry)){
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            }catch (Exception e){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            //进行等待时
            if(l != null){
                l.lock();
                l.unlock();
            }
            //如果该事务为删除该条记录（版本）的事务，则不能删除，防止重复删除，影响性能
            if(entry.getXmax() == xid){
                return false;
            }
            //如果存在版本跳跃
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            entry.setXmax(xid);
            return true;
        }finally {
            entry.release();
        }
    }

    /**
     * 开启版本控制
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null){
                throw t.err;
            }
        }catch (NullPointerException n){
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }
        //解除事务占用的一切锁和相应的资源，移出活跃事务列表
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }


    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted){
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        //如果不是自动中止
        if(!autoAborted){
            //移出当前活跃的事务
            activeTransaction.remove(xid);
        }
        lock.unlock();
        //如果事务t已经自动中止
        if(t.autoAborted) return;
        //还没有的话
        //取消相应的锁
        lt.remove(xid);
        //标识该事务已经结束
        tm.abort(xid);
    }
}
