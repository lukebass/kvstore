package com.s62023080.CPEN431.G4;

import java.net.InetAddress;

public class CacheData {
    public byte[] data;
    public InetAddress address;
    public int port;
    public long time;

    public CacheData(byte[] data, InetAddress address, int port, long time) {
        this.data = data;
        this.address = address;
        this.port = port;
        this.time = time;
    }
}
