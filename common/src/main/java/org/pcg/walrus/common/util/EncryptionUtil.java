package org.pcg.walrus.common.util;

import java.util.Base64;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class EncryptionUtil {

    /*
     * md5 : 32
     */
    public static String md5(String str) {
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.reset();
            messageDigest.update(str.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            // do nothing
        }
        byte[] byteArray = messageDigest.digest();
        StringBuffer md5StrBuff = new StringBuffer();
        for (int i = 0; i < byteArray.length; i++) {
            if (Integer.toHexString(0xFF & byteArray[i]).length() == 1)
                md5StrBuff.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
            else
                md5StrBuff.append(Integer.toHexString(0xFF & byteArray[i]));
        }
        return md5StrBuff.toString();
    }

    /**
     * to Ascii string
     */
    public static String toAsciiString(String str){
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < str.length(); i++){
            int chr1 = str.charAt(i);
            if(chr1>=19968&&chr1<=171941) sb.append("\\u" + Integer.toHexString(chr1));
            else sb.append(str.charAt(i));
        }
        return sb.toString();
    }

    /**
     * Base64 encode
     */
    public static String base64Encode(String data){
        return Base64.getEncoder().encodeToString(data.getBytes());
    }

    /**
     * Base64 decode
     */
    public static String base64Decode(String data) throws UnsupportedEncodingException {
        return new String(Base64.getDecoder().decode(data.getBytes()), "utf-8");
    }
}