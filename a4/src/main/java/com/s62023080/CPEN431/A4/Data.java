package com.s62023080.CPEN431.A4;

import com.google.protobuf.ByteString;

public class Data {
    public int version;

    public ByteString value;

    public Data(ByteString value, int version) {
        this.value = value;
        this.version = version;
    }
}
