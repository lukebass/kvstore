package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class Store {
    private HashMap<ByteBuffer, byte[]> store;

    private final static int SUCCESS = 0;

    private final static int MISSING_KEY_ERROR = 1;

    private final static int SPACE_ERROR = 2;

    private final static int STORE_ERROR = 4;

    private final static int INVALID_KEY_ERROR = 6;

    private final static int INVALID_VALUE_ERROR = 7;

    public Store() {
        this.store = new HashMap<>();
    }

    public int put(byte[] key, byte[] value, int version) {
        byte[] concat = new byte[value.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(concat);
        buffer.putInt(version);
        buffer.put(value);
        this.store.put(ByteBuffer.wrap(key), concat);
        return SUCCESS;
    }

    public int clear() {
        this.store = new HashMap<>();
        return SUCCESS;
    }
}
