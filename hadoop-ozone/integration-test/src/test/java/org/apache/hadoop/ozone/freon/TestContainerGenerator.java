package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertNotNull;

public class TestContainerGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerGenerator.class);

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster;
  private MiniOzoneCluster.Builder builder = null;

  @Before
  public void setup() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();

    path = GenericTestUtils
        .getTempPath(TestContainerGenerator.class.getSimpleName());
    // clean up before initialization
    FileUtils.deleteDirectory(new File(path));

    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, path);
    conf.set(HDDS_DATANODE_DIR_KEY, path);

    // default: file per block. Optionally file per chunk.
    //conf.setEnum(OZONE_SCM_CHUNK_LAYOUT_KEY, ChunkLayOutVersion.FILE_PER_CHUNK);

    // initialize directory for OM, SCM and DN.
    builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setBlockSize(1024);

    /*cluster = builder.build();
    cluster.waitForClusterToBeReady();


    cluster.stop();

     */
    //cluster.shutdown();
    //LOG.info("cluster shutdown. Let's make sure rocksdb is closed");
    //cluster.getStorageContainerManager().

  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() throws IOException {
  }

  @Test
  public void test() throws IOException, TimeoutException,
      InterruptedException {
    FileOutputStream out = FileUtils.openOutputStream(new File(path,
        "conf"));
    conf.writeXml(out);
    out.getFD().sync();
    out.close();

    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(
        new String[]{"-conf", confPath, "cg",
            "--user-id", "weichiu",
            "--cluster-id", "CID-376d737a-d05a-489d-880a-52814d047956",
            "--datanode-id", "b02c60b5-596a-463a-80c3-97ba395ae5e9",
            "--scm-id", "68551182-c2a1-4318-90e8-ad3d26555f9a",
            "--block-per-container", "10",
            "--size", "1024",
            "--om-key-batch-size", "10",
            "--write-dn",
            "--write-scm",
            "--write-om",
            "--repl", "1",
            "-t", "2",
            "-n", "100"
            });

    // TODO load up OM, SCM and DNs


    /*
    cluster = builder.build();
    cluster.waitForClusterToBeReady();

    OzoneClient client = cluster.getClient();
    Iterator<? extends OzoneVolume> it = client.getObjectStore().listVolumes("");
    while (it.hasNext()) {
      OzoneVolume vol = it.next();
      String volumeName = vol.getName();
      LOG.info("The cluster has volume {}", volumeName);
    }

    OzoneVolume volume = client.getObjectStore().getVolume(ContainerGenerator.getVolumeName());
    assertNotNull("Unable to find volume!", volume);
    OzoneBucket bucket = volume.getBucket(ContainerGenerator.getBucketName());
    assertNotNull("Unable to find bucket!", bucket);
    OzoneFileStatus fileStatus = bucket.getFileStatus("L1-0");
    assertNotNull("Unable to find file status of /L1-0!", fileStatus);


    cluster.shutdown();*/
  }
}
