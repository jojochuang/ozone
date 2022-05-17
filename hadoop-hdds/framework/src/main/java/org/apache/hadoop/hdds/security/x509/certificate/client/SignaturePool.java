package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.security.Signature;

public class SignaturePool extends GenericObjectPool<Signature> {

  public SignaturePool(PooledObjectFactory<Signature> factory,
      /*GenericObjectPoolConfig<Signature> config,*/
      String algorithm, String provider) {
    //super(factory, config);
    super(factory);

    GenericObjectPoolConfig<Signature> config = new GenericObjectPoolConfig<>();
    //config.setTestOnBorrow(true);
    config.setBlockWhenExhausted(true);
    config.setMaxTotal(10);

    super.setConfig(config);
  }
}
