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

    public void put(ByteString key, ByteString value, int version) {
        this.lock.writeLock().lock();
        try {
            this.store.put(key, new Data(value, version));
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
