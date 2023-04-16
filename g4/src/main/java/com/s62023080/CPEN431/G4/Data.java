package com.s62023080.CPEN431.G4;

import com.google.protobuf.ByteString;
import java.util.concurrent.ConcurrentHashMap;

public class Data {
    public int version;
    public ByteString value;
    ConcurrentHashMap<Integer, Long> clock;

    public Data(ByteString value, int version, ConcurrentHashMap<Integer, Long> clock) {
        this.value = value;
        this.version = version;
        this.clock = clock;
    }
}
