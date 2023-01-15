package com.s62023080.CPEN431.A1;

/**
 * Various static routines to help with strings
 */
public class StringUtils {

    public static String byteArrayToHexString(byte[] bytes) {
        StringBuffer buf = new StringBuffer();
        String str;
        int val;

        for (int i = 0; i < bytes.length; i++) {
            val = ((int) bytes[i]) & 0x000000FF;
            str = Integer.toHexString(val);
            while (str.length() < 2)
                str = "0" + str;
            buf.append(str);
        }

        return buf.toString().toUpperCase();
    }
}