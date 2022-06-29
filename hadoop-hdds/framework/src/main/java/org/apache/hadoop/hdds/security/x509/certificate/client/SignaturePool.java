/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.security.Signature;

/**
 * Creates a pool of Signature classes.
 * Objects are pre-allocated at initialization and are not evicted if idle.
 *
 * Due to the cost of object allocation at initialization, this pool is expected
 * to be long-lived.
 *
 * It is based on Apache Commons Pool. For more details, check out the
 * <a href="https://commons.apache.org/proper/commons-pool/">website</a>.
 */
public class SignaturePool extends GenericObjectPool<Signature> {

  private static class SignatureFactory
      implements PooledObjectFactory<Signature> {

    private String algorithm;
    private String provider;

    SignatureFactory(String signatureAlgorithm,
        String securityProvider) {
      this.algorithm = signatureAlgorithm;
      this.provider = securityProvider;
    }

    @Override
    public PooledObject<Signature> makeObject() throws Exception {
      Signature sign = Signature.getInstance(algorithm, provider);
      return new DefaultPooledObject<>(sign);
    }

    @Override
    public void destroyObject(PooledObject<Signature> pooledObject)
        throws Exception {
      // do nothing
    }

    @Override
    public boolean validateObject(
        PooledObject<Signature> pooledObject) {
      return false;
    }

    @Override
    public void activateObject(PooledObject<Signature> pooledObject)
        throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<Signature> pooledObject)
        throws Exception {

    }
  }

  public SignaturePool(PooledObjectFactory<Signature> factory,
      GenericObjectPoolConfig<Signature> config) {
    super(factory, config);
  }

  /**
   * Create a new instance of SignaturePool. Each SignaturePool can have
   * up to 10 Signature objects, and is blocked if none is available.
   * @param algorithm algorithm name of the signatures.
   * @param provider provider name of the signatures.
   * @return
   */
  public static SignaturePool newInstance(String algorithm, String provider) {
    GenericObjectPoolConfig<Signature> config = new GenericObjectPoolConfig<>();
    config.setBlockWhenExhausted(true);
    config.setMaxTotal(10);

    SignatureFactory factory = new SignatureFactory(algorithm, provider);

    return new SignaturePool(factory, config);
  }

  /**
   * When the pool is garbage collected, close the resources and destroy
   * the objects in the pool.
   */
  @Override
  protected void finalize() {
    this.close();
  }
}
