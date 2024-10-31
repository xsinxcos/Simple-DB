package com.zhuo.mydb.backend.vm;

import com.zhuo.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * vm对一个事务的抽象
 * @author : wzq
 **/
public class Transaction {
    //当前事务的xid
    public long xid;
    //事务的等级
    public int level;
    //快照
    public Map<Long, Boolean> snapshot;
    //错误
    public Exception err;

    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        //读已提交的时候不需要快照读，可重复读才需要
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        //判断事务xid是否在快照的活跃事务中
        return snapshot.containsKey(xid);
    }
}
