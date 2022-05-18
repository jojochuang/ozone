package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.security.Signature;

public class SignaturePool extends GenericObjectPool<Signature> {

  public static class SignatureFactory
      implements PooledObjectFactory<Signature> {

    private String algorithm;
    private String provider;

    public SignatureFactory(String signatureAlgorithm,
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

  public static SignaturePool newInstance(String algorithm, String provider) {

    GenericObjectPoolConfig<Signature> config = new GenericObjectPoolConfig<>();
    //config.setTestOnBorrow(true);
    config.setBlockWhenExhausted(true);
    config.setMaxTotal(10);

    SignatureFactory factory = new SignatureFactory(algorithm, provider);

    return new SignaturePool(factory, config);
  }
}
