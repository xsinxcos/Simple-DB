package com.zhuo.mydb.backend.vm;

import com.zhuo.mydb.backend.tm.TransactionManager;

/**
 * @author : wzq
 **/
public class Visibility {
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
        long xmax = e.getXmax();
        //读已提交的情况下，不存在快照读，没有版本问题
        if(t.level == 0){
            return false;
        }else {
            //该记录删除已提交 并且 （删除的 xid 发生在 该事务之后 或者 记录删除的事务在 当前事务的快照中还未提交），存在版本跳跃，解决不可重复读的问题
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e){
        if(t.level == 0){
            return readCommitted(tm, t, e);
        }else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmax = e.getXmax();
        long xmin = e.getXmin();
        //当前事务xid 等于创建该条记录（版本）的事务编号 xid 相等 时，并且 该条记录（版本）未删除，可读
        if(xid == xmin && xmax == 0){
            return true;
        }
        // 当 创建该条记录（版本）的事务编号 xid 已提交的时候
        if(tm.isCommitted(xmin)){
            // 该记录未删除时
            if(xmax == 0){
                return true;
            }
            // 当前事务不是删除该记录的事务的时候 ，删除该记录的事务未提交时可读
            if(xmax != xid){
                if(!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //当前事务xid 等于创建该条记录（版本）的事务编号 xid 相等 时，并且 该条记录（版本）未删除，可读
        if(xmin == xid && xmax == 0) return true;

        //1.创建该条记录（版本）的事务已提交 2. 当前事务id 大于 xmin（即为 时间在它之后）
        // 3.xmin在当前快照不是活跃的，再由1，2可以推出xmin在xid之前已经提交
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            //该记录未删除
            if(xmax == 0) return true;
            if(xmax != xid){
                //1. 在当前事务中还未提交 2. 当前事务id 小于 xmax（即为 时间在它之前） 3. xmax在当前快照是活跃的（相当于未提交）
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
