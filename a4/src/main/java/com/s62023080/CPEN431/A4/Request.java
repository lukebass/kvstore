package com.s62023080.CPEN431.A4;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Request {
    public InetAddress address;
    public int port;
    public byte[] data;

    public Request(DatagramPacket packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet.getData());
        byte[] request = new byte[packet.getLength()];
        buffer.get(request);
        this.data = request;
        this.address = packet.getAddress();
        this.port = packet.getPort();
    }
}
