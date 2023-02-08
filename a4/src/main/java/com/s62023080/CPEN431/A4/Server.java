package com.s62023080.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.net.*;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final ExecutorService executor;

    private final Store store;

    private final Cache<String, byte[]> cache;

    private final int waitTime;

    private boolean running;

    public Server(int port, int nThreads, int cacheExpiration, int waitTime) throws IOException {
        this.socket = new DatagramSocket(port);
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, TimeUnit.MILLISECONDS).build();
        this.waitTime = waitTime;
        this.running = true;
    }

    public Store getStore() {
        return this.store;
    }

    public Cache<String, byte[]> getCache() {
        return this.cache;
    }

    public void run() {
        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                this.socket.receive(packet);
                this.executor.submit(new ServerResponse(this.socket, packet, this.store, this.cache, this.waitTime));
                System.out.println(this.cache.size() + " / " + this.store.size() + " / " + Utils.getFreeMemory());
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
        this.executor.shutdown();
        this.clear();
    }
}
