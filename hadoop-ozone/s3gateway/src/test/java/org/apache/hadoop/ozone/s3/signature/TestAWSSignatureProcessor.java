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

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.AWS4_SIGNING_ALGORITHM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AWSSignatureProcessor}.
 */
public class TestAWSSignatureProcessor {

  private AWSSignatureProcessor signatureProcessor;
  private ContainerRequestContext mockContext;
  private UriInfo mockUriInfo;
  private SecretKeyProvider mockSecretKeyProvider;

  private static final String TEST_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";
  private static final String TEST_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  private static final String TEST_REGION = "us-east-1";
  private static final String TEST_SERVICE = "s3";
  private static final String TEST_DATE = ZonedDateTime.now().format(StringToSignProducer.TIME_FORMATTER);
  private static final String TEST_CREDENTIAL_SCOPE = String.format("%s/%s/%s/%s",
      TEST_DATE.substring(0, 8), TEST_REGION, TEST_SERVICE, "aws4_request");

  @BeforeEach
  public void setup() throws Exception {
    signatureProcessor = new AWSSignatureProcessor();
    mockContext = Mockito.mock(ContainerRequestContext.class);
    mockUriInfo = Mockito.mock(UriInfo.class);
    mockSecretKeyProvider = Mockito.mock(SecretKeyProvider.class);

    when(mockContext.getUriInfo()).thenReturn(mockUriInfo);
    when(mockUriInfo.getRequestUri()).thenReturn(new URI("http://localhost/testbucket/testkey?X-Amz-Signature=dummy"));
    when(mockSecretKeyProvider.getSecretKey(TEST_ACCESS_KEY)).thenReturn(TEST_SECRET_KEY);

    signatureProcessor.setSecretKeyProvider(mockSecretKeyProvider);
  }

  private MultivaluedMap<String, String> createQueryParams(String signature, String method) {
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.add("X-Amz-Algorithm", AWS4_SIGNING_ALGORITHM);
    queryParams.add("X-Amz-Credential", TEST_ACCESS_KEY + "/" + TEST_CREDENTIAL_SCOPE);
    queryParams.add("X-Amz-Date", TEST_DATE);
    queryParams.add("X-Amz-Expires", "3600"); // 1 hour expiration
    queryParams.add("X-Amz-SignedHeaders", "host");
    queryParams.add("X-Amz-Signature", signature);
    return queryParams;
  }

  private MultivaluedMap<String, String> createHeaders() {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.add("Host", "localhost");
    return headers;
  }

  @Test
  public void testValidatePresignedPutRequestSuccess() throws Exception {
    MultivaluedMap<String, String> queryParams = createQueryParams("", "PUT");
    MultivaluedMap<String, String> headers = createHeaders();

    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    when(mockContext.getHeaders()).thenReturn(headers);

    // Manually calculate the expected signature for a PUT request
    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setDate(TEST_DATE.substring(0, 8))
        .setDateTime(TEST_DATE)
        .setAwsAccessId(TEST_ACCESS_KEY)
        .setSignedHeaders("host")
        .setCredentialScope(TEST_CREDENTIAL_SCOPE)
        .setAlgorithm(AWS4_SIGNING_ALGORITHM)
        .setSignPayload(false)
        .setRegion(TEST_REGION)
        .setService(TEST_SERVICE)
        .build();

    String stringToSign = StringToSignProducer.createSignatureBase(
        signatureInfo,
        "http",
        "PUT",
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(headers),
        StringToSignProducer.fromMultiValueToSingleValueMap(queryParams));

    String expectedSignature = SignatureProcessorUtil.calculateSignature(
        TEST_SECRET_KEY, signatureInfo.getDate(), signatureInfo.getRegion(),
        signatureInfo.getService(), stringToSign);

    queryParams.putSingle("X-Amz-Signature", expectedSignature);

    assertDoesNotThrow(() -> signatureProcessor.validateRequest(mockContext, "PUT"));
  }

  @Test
  public void testValidatePresignedPutRequestInvalidSignature() throws Exception {
    MultivaluedMap<String, String> queryParams = createQueryParams("invalid-signature", "PUT");
    MultivaluedMap<String, String> headers = createHeaders();

    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    when(mockContext.getHeaders()).thenReturn(headers);

    assertThrows(OS3Exception.class, () -> signatureProcessor.validateRequest(mockContext, "PUT"));
  }

  @Test
  public void testValidatePresignedPutRequestExpired() throws Exception {
    MultivaluedMap<String, String> queryParams = createQueryParams("", "PUT");
    MultivaluedMap<String, String> headers = createHeaders();

    // Set an expired date
    String expiredDate = ZonedDateTime.now().minusDays(2).format(StringToSignProducer.TIME_FORMATTER);
    queryParams.putSingle("X-Amz-Date", expiredDate);

    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    when(mockContext.getHeaders()).thenReturn(headers);

    // Manually calculate the expected signature for a PUT request with expired date
    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setDate(expiredDate.substring(0, 8))
        .setDateTime(expiredDate)
        .setAwsAccessId(TEST_ACCESS_KEY)
        .setSignedHeaders("host")
        .setCredentialScope(String.format("%s/%s/%s/%s",
            expiredDate.substring(0, 8), TEST_REGION, TEST_SERVICE, "aws4_request"))
        .setAlgorithm(AWS4_SIGNING_ALGORITHM)
        .setSignPayload(false)
        .setRegion(TEST_REGION)
        .setService(TEST_SERVICE)
        .build();

    String stringToSign = StringToSignProducer.createSignatureBase(
        signatureInfo,
        "http",
        "PUT",
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(headers),
        StringToSignProducer.fromMultiValueToSingleValueMap(queryParams));

    String expectedSignature = SignatureProcessorUtil.calculateSignature(
        TEST_SECRET_KEY, signatureInfo.getDate(), signatureInfo.getRegion(),
        signatureInfo.getService(), stringToSign);

    queryParams.putSingle("X-Amz-Signature", expectedSignature);

    assertThrows(OS3Exception.class, () -> signatureProcessor.validateRequest(mockContext, "PUT"));
  }

  @Test
  public void testValidatePresignedPutRequestWrongMethod() throws Exception {
    MultivaluedMap<String, String> queryParams = createQueryParams("", "GET"); // Signature generated for GET
    MultivaluedMap<String, String> headers = createHeaders();

    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    when(mockContext.getHeaders()).thenReturn(headers);

    // Manually calculate the expected signature for a GET request
    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setDate(TEST_DATE.substring(0, 8))
        .setDateTime(TEST_DATE)
        .setAwsAccessId(TEST_ACCESS_KEY)
        .setSignedHeaders("host")
        .setCredentialScope(TEST_CREDENTIAL_SCOPE)
        .setAlgorithm(AWS4_SIGNING_ALGORITHM)
        .setSignPayload(false)
        .setRegion(TEST_REGION)
        .setService(TEST_SERVICE)
        .build();

    String stringToSign = StringToSignProducer.createSignatureBase(
        signatureInfo,
        "http",
        "GET", // Method used for signature generation
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(headers),
        StringToSignProducer.fromMultiValueToSingleValueMap(queryParams));

    String expectedSignature = SignatureProcessorUtil.calculateSignature(
        TEST_SECRET_KEY, signatureInfo.getDate(), signatureInfo.getRegion(),
        signatureInfo.getService(), stringToSign);

    queryParams.putSingle("X-Amz-Signature", expectedSignature);

    assertThrows(OS3Exception.class, () -> signatureProcessor.validateRequest(mockContext, "PUT")); // Validate with PUT
  }
}
