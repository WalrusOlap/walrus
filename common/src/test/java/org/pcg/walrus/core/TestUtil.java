package org.pcg.walrus.core;

import org.junit.Test;
import org.pcg.walrus.common.util.EncryptionUtil;
import static org.junit.Assert.assertTrue;

public class TestUtil {

    @Test
    public void TestEncrypt() throws Exception {
        String pwd = "password";
        String dec = EncryptionUtil.base64Decode(EncryptionUtil.base64Encode(pwd));
        System.out.println(EncryptionUtil.base64Encode(pwd));
        assertTrue(dec.equals(pwd));
    }
}
