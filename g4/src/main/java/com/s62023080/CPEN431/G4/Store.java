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
        Data data = new Data(value, version);
        this.lock.writeLock().lock();
        this.store.put(key, data);
        this.lock.writeLock().unlock();
    }

    public Data get(ByteString key) {
        this.lock.writeLock().lock();
        Data data = this.store.get(key);
        this.lock.writeLock().unlock();
        return data;
    }

    public Data remove(ByteString key) {
        this.lock.writeLock().lock();
        Data data = this.store.remove(key);
        this.lock.writeLock().unlock();
        return data;
    }

    public void bulkRemove(ArrayList<ByteString> keys) {
        this.lock.writeLock().lock();
        for (ByteString key : keys) this.store.remove(key);
        this.lock.writeLock().unlock();
    }

    public void clear() {
        this.store = new ConcurrentHashMap<>();
    }

    public int size() { return this.store.size(); }
}
