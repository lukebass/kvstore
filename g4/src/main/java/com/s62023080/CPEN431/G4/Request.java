package com.s62023080.CPEN431.G4;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Request {
    public byte[] data;
    public InetAddress address;
    public int port;

    public Request(DatagramPacket packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet.getData());
        byte[] request = new byte[packet.getLength()];
        buffer.get(request);
        this.data = request;
        this.address = packet.getAddress();
        this.port = packet.getPort();
    }
}
