package com.zhuo.mydb.backend.vm;

/**
 * @author : wzq
 **/

import com.zhuo.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;


    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 添加 xid 和 uid 之间 锁，能立刻添加就直接返回null ，不能就返回等待的锁
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try {
            //如果当前事务已经拥有该资源的时
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            //如果该 资源 没有被某个事务占有
            if (!u2x.containsKey(uid)) {
                //建立该事务和资源的持有的关系
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            //如果该 资源 被某个事务占有，则放入等待Map和列表中
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            //进行死锁检测，如果发生死锁了
            if(hasDeadLock()){
                //移除等待的事务
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            //将等待锁添加到相应的Map中
            Lock l = new ReentrantLock();
            waitLock.put(xid, l);
            return l;
        }finally {
            lock.unlock();
        }
    }

    public void remove(long xid){
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null){
                while (!l.isEmpty()){
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        }finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert !l.isEmpty();

        while(!l.isEmpty()) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.isEmpty()) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) return false;
        for (long e : l) {
            if (e == uid1) {
                return true;
            }
        }
        return false;
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.isEmpty()) {
            listMap.remove(uid0);
        }
    }
}
