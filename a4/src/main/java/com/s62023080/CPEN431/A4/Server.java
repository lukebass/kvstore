package com.s62023080.CPEN431.A4;

import java.io.IOException;
import java.net.*;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final Store store;

    private boolean running;

    public Server(int port) throws IOException {
        this.socket = new DatagramSocket(port);
        this.store = new Store();
        this.running = true;
    }

    public void run() {
        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                this.socket.receive(packet);
                new Thread(new ServerResponse(this.socket, packet, this.store)).start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void shutdown() {
        this.running = false;
    }
}
