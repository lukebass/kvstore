package com.s62023080.CPEN431.G4;

import java.util.concurrent.ConcurrentSkipListMap;

public class Logger {
    private final int pid;

    public Logger(int pid) {
        this.pid = pid;
    }

    public void log(String message) {
        System.out.println(message);
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("\n");
    }

    public void log(String message, int storeSize, long cacheSize, int queueSize) {
        System.out.println(message);
        System.out.println("Store: " + storeSize);
        System.out.println("Cache: " + cacheSize);
        System.out.println("Queue: " + queueSize);
        System.out.println("Memory: " + Utils.getFreeMemory());
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("\n");
    }

    public void log(ConcurrentSkipListMap<Integer, Integer> addresses) {
        System.out.println("Addresses Size: " + addresses.size());
        System.out.println("Node IDs:");
        System.out.println(addresses.keySet());
        System.out.println("Node Addresses:");
        System.out.println(addresses.values());
        System.out.println("\n");
    }
}
