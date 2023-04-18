package com.s62023080.CPEN431.G4;

import com.google.protobuf.ByteString;
import java.util.Map;

public class Data {
    public int version;
    public ByteString value;
    Map<Integer, Long> clocks;

    public Data(ByteString value, int version, Map<Integer, Long> clocks) {
        this.value = value;
        this.version = version;
        this.clocks= clocks;
    }
}
