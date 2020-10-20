package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "rr",
    aliases = "random-read",
    description = "Randomly read files created by ContainerGenerator",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class RandomRead  extends BaseFreonGenerator implements Callable<Void> {
  public static final Logger LOG =
      LoggerFactory.getLogger(RandomRead.class);

  @CommandLine.Option(names = {"--fileid-range" },
      description = "Range of File ID",
      defaultValue = "0")
  private long fileIdRange;

  @CommandLine.Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @CommandLine.Option(names = {"--volume" },
      description = "Volume name. Default: vol1",
      defaultValue = "vol1")
  private static String volumeName;

  @CommandLine.Option(names = {"--bucket" },
      description = "Bucket name. Default: bucket1",
      defaultValue = "bucket1")
  private static String bucketName;

  @CommandLine.Option(names = {"--randomize" },
      description = "Randomize read or sequential")
  private static boolean randomize;

  @CommandLine.Option(names = {"--size" },
      description = "Number of bytes to read in each key. Default: -1 means read till the end of file",
      defaultValue = "-1")
  private static int readSize;

  @CommandLine.Option(names = {"--op" },
      description = "operations to run: read, getfilestatus, lookup, getacl",
      defaultValue = "read")
  private static String operation;

  static final Random RANDOM = new Random();

  private OzoneClient ozoneClient;
  private RpcClient rpcClient;
  private Timer timer;

  private OzoneBucket bucket;

  @Override public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    try {
      rpcClient = new RpcClient(ozoneConfiguration, omServiceID);

      ozoneClient = createOzoneClient(omServiceID, ozoneConfiguration);
      bucket = ozoneClient.getObjectStore().getVolume(volumeName).getBucket(bucketName);
      timer = getMetrics().timer("random-read");

      runTests(this::writeContainer);

    } finally{
      if (ozoneClient != null) {
        ozoneClient.close();
      }

      if (rpcClient != null) {
        rpcClient.close();
      }

    }

    return null;
  }

  private void writeContainer(long n) throws Exception {
    // if randomize == true, use a random number, convert to a non-negative
    // number. Otherwise use n.
    long l = (randomize?(RANDOM.nextLong() & 0x0F_FF_FF_FF_FF_FF_FF_FFL):(n)) %
        fileIdRange;
    long l3n = l / 1_000 % 1_000;
    long l2n = l / 1_000_000 % 1_000;
    long l1n = l / 1_000_000_000 % 1_000;

    String level3 = "L3-" + l3n;
    String level2 = "L2-" + l2n;
    String level1 = "L1-" + l1n;

    String keyName = level1 + "/" + level2 + "/" + level3 + "/key" + l;
    //LOG.info("read key {} generated from number {}", keyName, l);

    timer.time(() -> {
      try {
        switch (operation) {
        case "getfilestatus":
          getFileStatus(keyName);
          break;
        case "liststatus":
          String dirName = level1 + "/" + level2 + "/" + level3;
          getListStatus(dirName);
          break;
        case "lookup":
          lookupFile(keyName);
          break;
        case "lookupkey":
          lookupKey(keyName);
          break;
        case "getacl":
          getAcl(keyName);
          break;
        case "read":
          readKey(keyName);
          break;
        }

      } catch (Exception e) {
        // ignore whatever errors
        e.printStackTrace();
      }
    });
  }

  // lookupKey
  // listAllVolumes
  // listVolumeByUser
  // listStatus
  // listKeys
  // listBuckets
  // getVolumeInfo
  // checkVolumeAccess

  // TODO: openKey

  private void getListStatus(String dirName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(dirName)
        .setSortDatanodesInPipeline(true)
        .build();
    rpcClient.getOzoneManagerClient().listStatus(keyArgs, true, "", 1000);
  }

  private void getFileStatus(String keyName)
      throws IOException {
    bucket.getFileStatus(keyName);
  }

  private void getAcl(String keyName)
      throws IOException {
    //bucket.getAcls();
    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName).build();
    rpcClient.getOzoneManagerClient().getAcl(ozoneObj);
  }

  private void lookupFile(String keyName)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(true)
        .build();
    OmKeyInfo keyInfo = rpcClient.getOzoneManagerClient().lookupFile(keyArgs);
  }

  private void lookupKey(String keyName)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(true)
        .build();
    OmKeyInfo keyInfo = rpcClient.getOzoneManagerClient().lookupKey(keyArgs);
  }

  private void readKey(String keyName)
      throws IOException {
    try (InputStream in = bucket.readKey(keyName)) {
      byte[] bytes = new byte[1048576];
      if (readSize == -1) {
        while (in.read(bytes) != -1) {
        }
      } else {
        int bytesToRead = readSize;
        while (bytesToRead > 1048576) {
          in.read(bytes, 0, 1048576);
          bytesToRead -= 1048576;
        }
        in.read(bytes, 0, bytesToRead);
      }
    }
  }
}