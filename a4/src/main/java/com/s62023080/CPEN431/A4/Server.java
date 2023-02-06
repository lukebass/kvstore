package com.s62023080.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.net.*;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final ExecutorService executor;

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private boolean running;

    public Server(int port, int expiration) throws IOException {
        this.socket = new DatagramSocket(port);
        this.executor = Executors.newCachedThreadPool();
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(expiration, TimeUnit.SECONDS).build();
        this.running = true;
    }

    public Store getStore() {
        return this.store;
    }

    public Cache<ByteString, byte[]> getCache() {
        return this.cache;
    }

    public void run() {
        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                this.socket.receive(packet);
                this.executor.submit(new ServerResponse(this.socket, packet, this.store, this.cache));
                System.out.println("Cache: " + this.cache.size());
                System.out.println("Store: " + this.store.size());
                System.out.println("Memory: " + Utils.isOutOfMemory());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void clear() {
        this.store.clear();
        this.cache.invalidateAll();
    }

    public void shutdown() {
        this.running = false;
        this.executor.shutdownNow();
        this.clear();
    }
}
