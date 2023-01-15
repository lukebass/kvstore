package com.s62023080.CPEN431.A1;

import java.net.*;
import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

public class Fetch {
    private final int ID_SIZE = 16;

    private final int PACKET_SIZE = 16000;

    private final DatagramSocket socket;

    private final int timeout;

    private final int retries;

    public Fetch(String host, String port, int timeout, int retries) throws SocketException, UnknownHostException {
        socket = new DatagramSocket();
        socket.connect(InetAddress.getByName(host), Integer.parseInt(port));
        this.timeout = timeout;
        socket.setSoTimeout(timeout);
        this.retries = retries;
    }

    public void close() {
        socket.close();
    }

    public void reset() throws SocketException {
        socket.setSoTimeout(this.timeout);
    }

    public void setTimeout(int timeout) throws SocketException {
        socket.setSoTimeout(timeout);
    }

    public byte[] createRequestId() {
        byte[] requestId = new byte[ID_SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(requestId);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // First 4 bytes are client IP
        buffer.put(socket.getLocalAddress().getAddress());
        // Next 2 bytes are client port
        buffer.putShort((short) socket.getLocalPort());
        // Next 2 bytes are random
        byte[] random = new byte[2];
        new Random().nextBytes(random);
        buffer.put(random);
        // Next 8 bytes are time
        buffer.putLong(System.nanoTime());

        return requestId;
    }

    public byte[] sendReceive(byte[] request) throws IOException {
        DatagramPacket requestPacket = new DatagramPacket(request, request.length);
        socket.send(requestPacket);
        DatagramPacket responsePacket = new DatagramPacket(new byte[PACKET_SIZE], PACKET_SIZE);
        socket.receive(responsePacket);
        return responsePacket.getData();
    }

    public byte[] fetch(byte [] request) throws IOException {
        byte[] formattedRequest = new byte[ID_SIZE + request.length];
        ByteBuffer buffer = ByteBuffer.wrap(formattedRequest);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // First 16 bytes are request ID
        byte[] requestId = createRequestId();
        buffer.put(requestId);
        // Next length bytes are request
        buffer.put(request);

        byte[] formattedResponse = null;
        int retries = this.retries;
        while (retries > 0) {
            try {
                byte[] response = sendReceive(formattedRequest);
                buffer = ByteBuffer.wrap(response);

                // First 16 bytes are response ID
                byte[] responseId = new byte[ID_SIZE];
                buffer.get(responseId);

                // Ensure request and response IDs match
                if (!Arrays.equals(requestId, responseId)) {
                    throw new SocketTimeoutException("Mismatched request and response IDs");
                }

                // Next length bytes are response
                formattedResponse = new byte[response.length - ID_SIZE];
                buffer.get(formattedResponse);

                break;
            } catch (SocketTimeoutException e) {
                setTimeout(socket.getSoTimeout() * 2);
                retries -= 1;
            }
        }

        reset();

        if (formattedResponse == null) {
            throw new HttpTimeoutException("Request Failed");
        }

        return formattedResponse;
    }
}