package com.s62023080.CPEN431.A4;

import java.io.IOException;
import java.net.*;

public class Server extends Thread {
    private final DatagramSocket socket;

    private final Store store;

    private final static int MAX_SIZE = 16000;

    public Server(int port) throws IOException {
        this.socket = new DatagramSocket(port);
        this.store = new Store();
    }

    public void run() {
        while(true) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[MAX_SIZE], MAX_SIZE);
                this.socket.receive(packet);
                new Thread(new ServerResponse(this.socket, packet, this.store)).start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
