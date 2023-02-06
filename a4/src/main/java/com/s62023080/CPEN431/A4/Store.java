package com.s62023080.CPEN431.A4;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Store {
    private ConcurrentHashMap<ByteString, byte[]> store;

    public Store() {
        this.store = new ConcurrentHashMap<>();
    }

    public void put(ByteString key, byte[] value, int version) {
        byte[] concat = new byte[value.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(concat);
        buffer.putInt(version);
        buffer.put(value);
        this.store.put(key, concat);
    }

    public byte[] get(ByteString key) {
        return this.store.get(key);
    }

    public byte[] remove(ByteString key) {
        return this.store.remove(key);
    }

    public void clear() {
        this.store = new ConcurrentHashMap<>();
    }

    public int size() { return this.store.size(); }
}
