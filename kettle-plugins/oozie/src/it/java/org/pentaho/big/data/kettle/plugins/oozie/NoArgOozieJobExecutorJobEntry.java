/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.oozie;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.mockito.Mockito;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.impl.shim.oozie.OozieServiceFactoryImpl;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.job.entries.oozie.OozieClientImpl;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieClientFactory;
import org.pentaho.oozie.shim.api.OozieJob;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;

@JobEntry( id = "OozieJobExecutor", name = "Oozie.JobExecutor.PluginName",
  description = "Oozie.JobExecutor.PluginDescription",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData", image = "oozie-job-executor.svg",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Oozie+Job+Executor",
  i18nPackageName = "org.pentaho.di.job.entries.oozie", version = "1" )
public class NoArgOozieJobExecutorJobEntry extends OozieJobExecutorJobEntry {

  private static final HadoopConfigurationProvider provider = initProvider();

  public NoArgOozieJobExecutorJobEntry() throws FileSystemException, ConfigurationException {
    super( new NamedClusterManager(), mock( RuntimeTestActionService.class ), mock( RuntimeTester.class ), initNamedClusterServiceLocator() );
  }

  private static NamedClusterServiceLocator initNamedClusterServiceLocator() throws ConfigurationException {
    NamedClusterServiceLocatorImpl namedClusterServiceLocator = new NamedClusterServiceLocatorImpl( mock( ClusterInitializer.class ) );
    namedClusterServiceLocator.factoryAdded( new OozieServiceFactoryImpl( true, provider.getConfiguration( null ) ),
      Collections.emptyMap() );
    return namedClusterServiceLocator;
  }

  public static HadoopConfigurationProvider getProvider() {
    return provider;
  }

  private static HadoopConfigurationProvider initProvider() {
    try {
      return new TestProvider();
    } catch ( FileSystemException e ) {
      e.printStackTrace();
      return null;
    }
  }

  static class TestProvider implements HadoopConfigurationProvider {
    HadoopConfiguration config;

    TestProvider() throws FileSystemException {
      config = new HadoopConfiguration( VFS.getManager().resolveFile( "ram:///" ), "test", "test",
        Mockito.<HadoopShim>mock( HadoopShim.class ), new TestOozieShim(), new NoArgOozieClientFactory() );
    }

    @Override
    public boolean hasConfiguration( String id ) {
      return true;
    }

    @Override
    public List<? extends HadoopConfiguration> getConfigurations() {
      return Arrays.asList( config );
    }

    @Override
    public HadoopConfiguration getConfiguration( String id ) throws ConfigurationException {
      return config;
    }

    @Override
    public HadoopConfiguration getActiveConfiguration() throws ConfigurationException {
      return config;
    }

    private class TestOozieShim implements PentahoHadoopShim {
      @Override public ShimVersion getVersion() {
        return null;
      }
    }
  }

  public static class NoArgOozieClientFactory implements OozieClientFactory {

    @Override public org.pentaho.oozie.shim.api.OozieClient create( String s ) {
      return new OozieClientImpl( OozieJobExecutorJobEntryIT.oozieClient ) {
        @Override
        public OozieJob run( Properties props ) throws OozieClientException {
          OozieJob job = super.run( props );
          return  new OozieJob() {
            @Override
            public String getId() {
              return job.getId();
            }

            @Override
            public boolean isRunning() throws OozieClientException {
              return job.isRunning();
            }

            @Override
            public boolean didSucceed() throws OozieClientException {
              return job.didSucceed();
            }

            @Override
            public String getJobLog() throws OozieClientException {
              return "All Ok";
            }
          };
        }
      };
    }

    @Override public ShimVersion getVersion() {
      return new ShimVersion( 0, 0 );
    }
  }
}
