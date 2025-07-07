/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.signature;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Utility class for signature processing.
 */
public final class SignatureProcessorUtil {

  private static final String ALGORITHM = "AWS4-HMAC-SHA256";
  private static final String HMAC_SHA256 = "HmacSHA256";

  private SignatureProcessorUtil() {
    // Utility class
  }

  public static String calculateSignature(String secretKey,
                                          String date,
                                          String region,
                                          String service,
                                          String stringToSign) throws NoSuchAlgorithmException, InvalidKeyException {
    byte[] kSecret = ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = HmacSHA256(kSecret, date);
    byte[] kRegion = HmacSHA256(kDate, region);
    byte[] kService = HmacSHA256(kRegion, service);
    byte[] kSigning = HmacSHA256(kService, "aws4_request");

    byte[] signature = HmacSHA256(kSigning, stringToSign);
    return Base64.getEncoder().encodeToString(signature).toLowerCase();
  }

  private static byte[] HmacSHA256(byte[] key, String data) throws NoSuchAlgorithmException, InvalidKeyException {
    Mac mac = Mac.getInstance(HMAC_SHA256);
    mac.init(new SecretKeySpec(key, HMAC_SHA256));
    return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
  }
}
