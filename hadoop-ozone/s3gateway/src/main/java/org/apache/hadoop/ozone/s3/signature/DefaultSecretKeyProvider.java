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

import javax.enterprise.context.ApplicationScoped;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Default implementation of SecretKeyProvider.
 * For testing purposes, returns a hardcoded secret key.
 * In a real deployment, this would fetch the secret key from Ozone Manager.
 */
@ApplicationScoped
public class DefaultSecretKeyProvider implements SecretKeyProvider {

  // TODO: Replace with actual lookup from Ozone Manager
  private static final String HARDCODED_SECRET_KEY = "YOUR_HARDCODED_SECRET_KEY";

  @Override
  public String getSecretKey(String awsAccessId) throws OS3Exception {
    // For now, return a hardcoded secret key. In a real implementation,
    // this would involve fetching the secret key associated with the awsAccessId
    // from Ozone Manager or a secure store.
    if ("AKIAIOSFODNN7EXAMPLE".equals(awsAccessId)) {
      return "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    } else if ("AKIAI44QH8DHBEXAMPLE".equals(awsAccessId)) {
      return "je7MtGbClwBF/2Zp9Utk/h3yCoEXAMPLEKEY";
    } else {
      throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, awsAccessId);
    }
  }
}
