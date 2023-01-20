package com.s62023080.CPEN431.A2;

import ca.NetSysLab.ProtocolBuffers.RequestPayload.ReqPayload;
import ca.NetSysLab.ProtocolBuffers.ResponsePayload.ResPayload;
import java.io.IOException;

public class App {
    public static byte[] generateReqPayload(String studentId) {
        ReqPayload.Builder reqPayload = ReqPayload.newBuilder();
        reqPayload.setStudentID(Integer.parseInt(studentId));
        return reqPayload.build().toByteArray();
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("This requires an address, port, and student ID");
            return;
        }

        try {
            Fetch fetch = new Fetch(args[0], args[1], 100, 4);
            ResPayload resPayload = ResPayload.parseFrom(fetch.fetch(generateReqPayload(args[2])));

            System.out.println("Student ID: " + args[2]);
            System.out.println("Secret Code Length: " + resPayload.getSecretKey().size());
            System.out.println("Secret Code: " + StringUtils.byteArrayToHexString(resPayload.getSecretKey().toByteArray()));

            fetch.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}