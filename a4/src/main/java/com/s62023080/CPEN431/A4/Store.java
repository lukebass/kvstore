package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class Store {
    private HashMap<ByteBuffer, byte[]> store;

    public Store() {
        this.store = new HashMap<>();
    }

    public void put(byte[] key, byte[] value, int version) {
        byte[] concat = new byte[value.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(concat);
        buffer.putInt(version);
        buffer.put(value);
        this.store.put(ByteBuffer.wrap(key), concat);
    }

    public byte[] get(byte[] key) {
        return this.store.get(ByteBuffer.wrap(key));
    }

    public byte[] remove(byte[] key) {
        return this.store.remove(ByteBuffer.wrap(key));
    }

    public void clear() {
        this.store = new HashMap<>();
    }
}
