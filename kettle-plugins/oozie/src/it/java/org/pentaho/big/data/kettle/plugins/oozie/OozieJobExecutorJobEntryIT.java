package org.pentaho.big.data.kettle.plugins.oozie;

import static org.junit.Assert.assertTrue;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.MRLocalCluster;
import com.github.sakserv.minicluster.impl.OozieLocalServer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * Created by Ihar_Chekan on 1/17/2017.
 */
public class OozieJobExecutorJobEntryIT {

  private static OozieLocalServer oozieLocalServer;
  private static HdfsLocalCluster hdfsLocalCluster;
  private static MRLocalCluster mrLocalCluster;
  protected static OozieClient oozieClient;

  @BeforeClass
  public static void setUp() throws Exception {
    // starting mini-clusters
    hdfsLocalCluster = new HdfsLocalCluster.Builder()
      .setHdfsNamenodePort( 12345 )
      .setHdfsNamenodeHttpPort( 12341 )
      .setHdfsTempDir( "target/embedded_hdfs" )
      .setHdfsNumDatanodes( 1 )
      .setHdfsEnablePermissions( false )
      .setHdfsFormat( true )
      .setHdfsEnableRunningUserAsProxyUser( true )
      .setHdfsConfig( new Configuration() )
      .build();
    hdfsLocalCluster.start();
    mrLocalCluster = new MRLocalCluster.Builder()
      .setNumNodeManagers( 1 )
      .setJobHistoryAddress( "localhost:37005" )
      .setResourceManagerAddress( "localhost:37001" )
      .setResourceManagerHostname( "localhost" )
      .setResourceManagerSchedulerAddress( "localhost:37002" )
      .setResourceManagerResourceTrackerAddress( "localhost:37003" )
      .setResourceManagerWebappAddress( "localhost:37004" )
      .setUseInJvmContainerExecutor( false )
      .setConfig( new Configuration() )
      .build();
    mrLocalCluster.start();

    File oozieHdfsShareLib = new File( "target/oozie_share_lib" );
    FileUtils.forceMkdir( oozieHdfsShareLib );
    oozieLocalServer = new OozieLocalServer.Builder()
      .setOozieTestDir( "target/embedded_oozie" )
      .setOozieHomeDir( "oozie_home" )
      .setOozieUsername( "oozie" )
      .setOozieGroupname( "users" )
      .setOozieYarnResourceManagerAddress( "localhost:37001" )
      .setOozieHdfsDefaultFs( "file:///" )
      .setOozieHdfsShareLibDir( oozieHdfsShareLib.getAbsolutePath() )
      .setOozieShareLibCreate( Boolean.TRUE )
      .setOozieLocalShareLibCacheDir( "share_lib_cache" )
      .setOoziePurgeLocalShareLibCache( Boolean.FALSE )
      .setOozieConf( new Configuration() )
      .build();
    oozieLocalServer.start();

    Field providerField = HadoopConfigurationBootstrap.class.getDeclaredField( "provider" );
    providerField.setAccessible( true );
    providerField.set( HadoopConfigurationBootstrap.getInstance(), NoArgOozieJobExecutorJobEntry.getProvider() );

    System.setProperty( "KETTLE_PLUGIN_CLASSES", NoArgOozieJobExecutorJobEntry.class.getCanonicalName() );

    KettleEnvironment.init();

    oozieClient = oozieLocalServer.getOozieClient();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    oozieLocalServer.stop();
    mrLocalCluster.stop();
    hdfsLocalCluster.stop();
  }

  @Test
  public void testRegressionOozieSimpleMiniCluster( ) throws Exception {
    File targetDir = new File( "target/oozieTest" );
    FileUtils.deleteDirectory( targetDir );
    FileUtils.copyDirectory( new File( "src/it/resources/oozie" ), targetDir );

    //creating job.properties
    Properties conf = oozieClient.createConfiguration();
    conf.setProperty( OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName() );
    conf.setProperty( "queueName", "default" );
    conf.setProperty( "oozie.wf.application.path", "file://" + targetDir.getAbsolutePath() );
    File job_properties = new File( targetDir.getAbsolutePath(), "job.properties" );
    FileOutputStream fileOutputStream = new FileOutputStream( job_properties );
    conf.store( fileOutputStream, "" );
    fileOutputStream.close();
    System.setProperty( "PATH_TO_OOZIE_JOB_PROPERTIES", targetDir.getAbsolutePath() + "/job.properties" );

    JobMeta meta = new JobMeta( "target/oozieTest/oozieTest.kjb", null );

    Job job = new Job( null, meta );

    job.start();
    job.waitUntilFinished();
    assertTrue( job.getResult().getResult() );
  }
}
