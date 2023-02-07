package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Store {
    private ConcurrentHashMap<Key, byte[]> store;

    public Store() {
        this.store = new ConcurrentHashMap<>();
    }

    public void put(Key key, byte[] value, int version) {
        byte[] composite = new byte[value.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(composite);
        buffer.putInt(version);
        buffer.put(value);
        this.store.put(key, composite);
    }

    public byte[] get(Key key) {
        return this.store.get(key);
    }

    public byte[] remove(Key key) {
        return this.store.remove(key);
    }

    public void clear() {
        this.store = new ConcurrentHashMap<>();
    }

    public int size() { return this.store.size(); }
}
