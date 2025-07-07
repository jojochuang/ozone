---
title: "S3 Presigned URL for Object Upload"
date: 2025-07-07
summary: "Design document for implementing S3 presigned URL uploads in Ozone."
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

### **Design Doc: S3 Presigned URL for Object Upload**

**1. Jira:**
*   **HDDS-5195:** S3Gateway: Add support for presigned URL for object upload

**2. Abstract**
This document proposes a design to implement presigned URL generation for object uploads (`PUT`) in Ozone's S3 Gateway. This feature will complement the existing presigned URL download (`GET`) functionality, bringing Ozone's S3 API closer to AWS S3's capabilities. A client will be able to generate a temporary, credential-less URL that can be used by another party to upload an object to a specific bucket and key. The permissions for the upload will be inherited from the user who generates the URL.

**3. Motivation**
Currently, Ozone's S3 Gateway supports presigned URLs for downloading objects but not for uploading them. This is a significant feature gap compared to AWS S3. Implementing presigned uploads provides a secure mechanism for applications to grant temporary, specific upload permissions to clients or users without distributing long-term access keys or credentials. This is a common pattern for applications where end-users upload content directly, such as image or video uploads.

**4. Proposed Changes**
The implementation will mirror the existing presigned URL download feature, extending it to support the `PUT` HTTP method.

**4.1. S3Gateway: Signature Generation**
The core signing logic (within `StringToSignProducer.java`) has been updated to handle the `PUT` method. The client-side generation of presigned URLs is handled by the AWS SDK, which constructs the URL with the necessary signature parameters.

**4.2. S3Gateway: Request Handling and Validation**
The S3 Gateway's object endpoint (`ObjectEndpoint.java`) has been modified to handle `PUT` requests that contain AWS signature information in the query parameters.

*   **Endpoint Logic:** The existing `PUT` handler for objects has been enhanced to detect if the request is a presigned request (by checking for `X-Amz-Signature` in the query parameters).
*   **Signature Validation:** If it is a presigned request, the gateway will:
    1.  Extract the `AWSAccessKeyId` from the query parameters.
    2.  Fetch the secret key associated with that access key (via `SecretKeyProvider`).
    3.  Reconstruct the "string-to-sign" using the `PUT` method, the canonical request details, and any signed headers from the incoming request.
    4.  Recalculate the signature and compare it with the one provided in the URL (`X-Amz-Signature`).
*   **Permission Check:** Upon successful signature validation, the gateway relies on the Ozone Manager to perform the ACL check for `s3:PutObject` permission, as the `awsAccessId` from the `SignatureInfo` is used to set the `S3Auth` context for the `ClientProtocol`.

**5. API Changes**

*   **REST API:** The `PUT /{bucket}/{key}` endpoint now accepts AWS Signature V4 query parameters for authentication.
    *Example Request:*
    ```http
    PUT /my-bucket/my-object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Date=...&X-Amz-Expires=...&X-Amz-SignedHeaders=host&X-Amz-Signature=... HTTP/1.1
    Host: s3g.example.com
    Content-Type: text/plain

    [file content]
    ```
*   **Java Client API:** The generation of presigned URLs is handled by the AWS SDK. Ozone's S3 Gateway now correctly processes these presigned `PUT` requests.

**6. Security Considerations**
*   The security model remains consistent with existing S3 functionality. Permissions are delegated from the URL creator.
*   The connection must be over HTTPS to prevent the uploaded data from being intercepted in transit.
*   Setting a reasonably short expiration time for the URL is crucial to limit the window of opportunity for misuse if the URL is leaked.
*   By signing headers like `Content-MD5`, the creator can ensure the integrity of the uploaded data.

**7. Testing Plan**
A comprehensive testing strategy has been implemented.
*   **Unit Tests:**
    *   `TestAWSSignatureProcessor.java` includes tests for signature validation logic for incoming presigned `PUT` requests, including cases with invalid signatures, expired URLs, and incorrect HTTP methods.
*   **Integration Tests (`robot` tests):**
    *   `AbstractS3SDKV1Tests.java` includes `testPresignedUrlPut` which generates a presigned `PUT` URL using the AWS SDK, uploads a file using the generated URL via `HttpUrlConnection`, and verifies the object's content in Ozone.

**8. Proposed Sub-tasks**
This is a medium-sized feature that has been broken down into the following sub-tasks:

*   **HDDS-13404:** S3Gateway: Implement signature generation logic for PUT presigned URLs. (Completed)
*   **HDDS-13405:** S3Gateway: Add endpoint logic to handle and validate incoming PUT presigned URL requests. (Completed)
*   **HDDS-13406:** S3Gateway: Expose a client API to generate PUT presigned URLs. (Completed - client-side generation is handled by AWS SDK, server-side processing is implemented)
*   **HDDS-13407:** S3Gateway: Add integration and unit tests for presigned URL uploads. (Completed)
*   **HDDS-13408:** [Docs] Document the S3 presigned URL upload feature. (In Progress)