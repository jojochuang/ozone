package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;

public class TestContainerGenerator {

  private String path;
  private OzoneConfiguration conf = null;

  @Before
  public void setup() throws IOException {

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
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() throws IOException {
  }

  @Test
  public void test() throws IOException {
    FileOutputStream out = FileUtils.openOutputStream(new File(path,
        "conf"));
    conf.writeXml(out);
    out.getFD().sync();
    out.close();

    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(
        new String[]{"-conf", confPath, "cg",
            "--datanode-id", "b02c60b5-596a-463a-80c3-97ba395ae5e9",
            "--scm-id", "68551182-c2a1-4318-90e8-ad3d26555f9a",
            "--block-per-container", "1000000",
            "--size", "1024",
            "--om-key-batch-size", "1000",
            //"--write-datanode",
            //"--write-scm",
            //"--write-om",
            "-t", "20",
            "-n", "10000000"
            });

    // TODO load up OM, SCM and DNs

  }
}
