package com.s62023080.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.net.*;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private boolean running;

    public Server(int port) throws IOException {
        this.socket = new DatagramSocket(port);
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(1000, TimeUnit.MILLISECONDS).build();
        this.running = true;
    }

    public void run() {
        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                this.socket.receive(packet);
                new Thread(new ServerResponse(this.socket, packet, this.store, this.cache)).start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void shutdown() {
        this.running = false;
    }
}
