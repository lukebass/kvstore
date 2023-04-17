package com.s62023080.CPEN431.G4;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.protobuf.ByteString;

public class Store {
    private ConcurrentHashMap<ByteString, Data> store;
    private final ReentrantReadWriteLock lock;

    public Store() {
        this.store = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public ConcurrentHashMap.KeySetView<ByteString, Data> getKeys() {
        return this.store.keySet();
    }

    public ConcurrentHashMap<Integer, Long> put(ByteString key, ByteString value, int version, ConcurrentHashMap<Integer, Long> clocks) {
        ConcurrentHashMap<Integer, Long> clone = new ConcurrentHashMap<>(clocks);
        this.lock.writeLock().lock();
        try {
            if (this.store.containsKey(key)) {
                ConcurrentHashMap<Integer, Long> dataClocks = this.store.get(key).clocks;
                for (int clock : dataClocks.keySet()) {
                    if (clone.containsKey(clock) && clone.get(clock) < dataClocks.get(clock)) return clocks;
                    else if (!clone.containsKey(clock)) clone.put(clock, dataClocks.get(clock));
                }
            }

            if (!clocks.containsKey(0)) clone.put(0, clone.get(0) == null ? 1L : clone.get(0) + 1);
            this.store.put(key, new Data(value, version, clone));
            return clone;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Data get(ByteString key) {
        this.lock.readLock().lock();
        try {
            return this.store.get(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void remove(ByteString key, ConcurrentHashMap<Integer, Long> clocks) {
        this.lock.writeLock().lock();
        try {
            if (this.store.containsKey(key)) {
                ConcurrentHashMap<Integer, Long> dataClocks = this.store.get(key).clocks;
                for (int clock : dataClocks.keySet()) {
                    if (clocks.containsKey(clock) && clocks.get(clock) < dataClocks.get(clock)) return;
                }
            }
            this.store.remove(key);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void bulkRemove(ArrayList<ByteString> keys) {
        this.lock.writeLock().lock();
        try {
            for (ByteString key : keys) this.store.remove(key);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void clear() {
        this.store = new ConcurrentHashMap<>();
    }

    public int size() { return this.store.size(); }
}
