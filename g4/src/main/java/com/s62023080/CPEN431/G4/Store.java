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
        this.lock.readLock().lock();
        try {
            return this.store.keySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Data put(ByteString key, ByteString value, int version, ConcurrentHashMap<Integer, Long> clocks) {
        this.lock.writeLock().lock();
        try {
            ConcurrentHashMap<Integer, Long> clone = new ConcurrentHashMap<>(clocks);

            if (this.store.containsKey(key)) {
                Data data = this.store.get(key);
                for (int clock : data.clocks.keySet()) {
                    if (clone.containsKey(clock) && clone.get(clock) < data.clocks.get(clock)) return data;
                    else if (!clone.containsKey(clock)) clone.put(clock, data.clocks.get(clock));
                }
            }

            if (!clocks.containsKey(0)) clone.put(0, clone.containsKey(0) ? clone.get(0) + 1 : 1);
            Data data = new Data(value, version, clone);
            this.store.put(key, data);
            return data;
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

    public Data remove(ByteString key) {
        this.lock.writeLock().lock();
        try {
            return this.store.remove(key);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void bulkRemove(ArrayList<ByteString> keys) {
        if (keys.size() == 0) return;
        this.lock.writeLock().lock();
        try {
            for (ByteString key : keys) this.store.remove(key);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void clear() {
        this.lock.writeLock().lock();
        try {
            this.store = new ConcurrentHashMap<>();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public int size() { return this.store.size(); }
}
