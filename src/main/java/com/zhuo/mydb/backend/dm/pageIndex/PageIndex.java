package com.zhuo.mydb.backend.dm.pageIndex;

import com.zhuo.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : wzq
 **/

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List[] lists;


    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    @SuppressWarnings("unchecked")
    public void add(int pgno, int freeSpace){
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno ,freeSpace));
        }finally {
            lock.unlock();
        }
    }

    /**
     * 检索出哪一页有足够的空间
     * @param spaceSize 空间大小
     * @return
     */
    public PageInfo select(int spaceSize) {
        // 因为每页都是共享资源，所以加锁保证数据不丢失
        lock.lock();
        //找出哪一页有空余的位置进行存储
        try {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].isEmpty()) {
                    number ++;
                    continue;
                }
                return (PageInfo) lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }


}
