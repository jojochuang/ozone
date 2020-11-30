package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "scmbench",
    aliases = "scmbenchmark",
    description = "Benchmark for SCM RPCs",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class SCMBenchmark extends BaseFreonGenerator implements Callable<Void> {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMBenchmark.class);

  private ContainerOperationClient containerOperationClient;
  private Timer timer;

  @CommandLine.Option(names = {"--op" },
      description = "operations to run: getcontainer, getcontainerwithpipeline, listcontainer",
      defaultValue = "read")
  private static String operation;

  @CommandLine.Option(names = {"--count" },
      description = "count. used by listcontainer, getcontainerwithpipelinebatch",
      defaultValue = "1")
  private static int count;

  @CommandLine.Option(names = {"--container-ranger" },
      description = "Range of Container ID",
      defaultValue = "0")
  private long containerRange;

  private Random RANDOM = new Random();

  @Override public Void call() throws Exception {
    init();
    OzoneConfiguration conf = createOzoneConfiguration();

    try {

      containerOperationClient = new ContainerOperationClient(conf);
      timer = getMetrics().timer("random-read");

      runTests(this::writeContainer);

    } finally{
      if (containerOperationClient != null) {
        containerOperationClient.close();
      }
    }
    return null;
  }

  private void writeContainer(long n) throws Exception {
    timer.time(() -> {
      try {
        long containerId = RANDOM.nextInt() % containerRange;
        switch (operation) {
          case "getcontainer":
            containerOperationClient.getContainer(containerId);
            break;
          case "getcontainerwithpipeline":
            containerOperationClient.getContainerWithPipeline(containerId);
            break;
          case "getcontainerwithpipelinebatch":
            containerOperationClient.getContainerWithPipeline(containerId);
            List<Long> containerIDs = new ArrayList<>();
            for (int i = 0; i < count; i++) {
              long randLong = RANDOM.nextInt() % containerRange;
              containerIDs.add(randLong);
            }
            containerOperationClient.getStorageContainerLocationProtocol().
                getContainerWithPipelineBatch(containerIDs);
            break;
          case "listcontainer":
            containerOperationClient.listContainer(containerId, count);
            break;
            case "readcontainer":
          containerOperationClient.readContainer(containerId);
          break;
        case "listpipelines":
          containerOperationClient.listPipelines();
          break;

        }
      } catch (Exception e) {
        // ignore whatever errors
        e.printStackTrace();
      }
    });
  }
}
