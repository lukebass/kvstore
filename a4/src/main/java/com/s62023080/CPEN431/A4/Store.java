package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.protobuf.ByteString;

public class Store {
    private ConcurrentHashMap<ByteString, byte[]> store;

    private final ReentrantReadWriteLock lock;

    public Store() {
        this.store = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public void put(ByteString key, byte[] value, int version) {
        byte[] composite = new byte[value.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(composite);
        buffer.putInt(version);
        buffer.put(value);
        lock.writeLock().lock();
        this.store.put(key, composite);
        lock.writeLock().unlock();
    }

    public byte[] get(ByteString key) {
        lock.readLock().lock();
        byte[] value = this.store.get(key);
        lock.readLock().unlock();
        return value;
    }

    public byte[] remove(ByteString key) {
        return this.store.remove(key);
    }

    public void clear() {
        this.store = new ConcurrentHashMap<>();
    }

    public int size() { return this.store.size(); }
}
