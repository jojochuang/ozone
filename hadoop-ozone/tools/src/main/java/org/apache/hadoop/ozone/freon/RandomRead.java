package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
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

  static final Random RANDOM = new Random();


  private OzoneClient rpcClient;
  private Timer timer;

  private OzoneBucket bucket;

  @Override public Void call() throws Exception {
    init();
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);

    timer = getMetrics().timer("random-read");

    bucket = rpcClient.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);

    runTests(this::writeContainer);

    return null;
  }

  private void writeContainer(long n) throws Exception {
    long l = (RANDOM.nextLong() & 0x0F_FF_FF_FF_FF_FF_FF_FFL) % fileIdRange;
    long l4n = l % 1_000;
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
        readKey(keyName);
      } catch (Exception e) {
        // ignore whatever errors
        e.printStackTrace();
      }
    });
  }

  private void readKey(String keyName)
      throws IOException {
    try (InputStream in = bucket.readKey(keyName)) {
      byte[] bytes = new byte[1048576];
      while (in.read(bytes) != -1) {}
    }
  }
}