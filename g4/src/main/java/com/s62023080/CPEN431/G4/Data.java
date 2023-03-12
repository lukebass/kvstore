package com.s62023080.CPEN431.G4;

import com.google.protobuf.ByteString;

public class Data {
    public int version;
    public ByteString value;

    public Data(ByteString value, int version) {
        this.value = value;
        this.version = version;
    }
}
