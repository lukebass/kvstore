package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Server extends Thread {
    private final DatagramSocket socket;

    private boolean running;

    public Server(String port) throws SocketException {
        socket = new DatagramSocket(Integer.parseInt(port));
        running = true;
    }

    public long createCheckSum(byte[] messageID, byte[] payload) {
        byte[] checkSum = new byte[messageID.length + payload.length];
        ByteBuffer buffer = ByteBuffer.wrap(checkSum);
        // First bytes are message ID
        buffer.put(messageID);
        // Next bytes are payload
        buffer.put(payload);
        CRC32 crc = new CRC32();
        crc.update(checkSum);
        return crc.getValue();
    }

    public byte[] receive() throws IOException {
        DatagramPacket responsePacket = new DatagramPacket(new byte[16000], 16000);
        socket.receive(responsePacket);
        ByteBuffer buffer = ByteBuffer.wrap(responsePacket.getData());
        byte[] response = new byte[responsePacket.getLength()];
        buffer.get(response);
        return response;
    }

    public void run() {
        while (running) {
            try {
                Message.Msg reqMsg = Message.Msg.parseFrom(receive());

                // Ensure checksum is valid
                if (reqMsg.getCheckSum() != createCheckSum(reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                    continue;
                }

                // Continue

            } catch (IOException e) {
                e.printStackTrace();
                running = false;
            }
        }

        socket.close();
    }
}
