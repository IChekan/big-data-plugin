/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.sqoop;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.kettle.plugins.sqoop.ui.AbstractSqoopJobEntryDialog;
import org.pentaho.big.data.kettle.plugins.sqoop.ui.SqoopImportJobEntryController;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;

/**
 * Dialog for the Sqoop Import Job Entry
 * 
 * @see SqoopImportJobEntry
 */
public class SqoopImportJobEntryDialog extends AbstractSqoopJobEntryDialog<SqoopImportConfig, SqoopImportJobEntry> {

  public SqoopImportJobEntryDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException {
    super( parent, jobEntry, rep, jobMeta );
  }

  @Override
  protected Class<?> getMessagesClass() {
    return SqoopImportJobEntry.class;
  }

  @Override
  protected SqoopImportJobEntryController createController( XulDomContainer container, SqoopImportJobEntry jobEntry,
                                                            BindingFactory bindingFactory ) {
    return new SqoopImportJobEntryController( jobMeta, container, jobEntry, bindingFactory );
  }

  @Override
  protected String getXulFile() {
    return "org/pentaho/big/data/kettle/plugins/sqoop/xul/SqoopImportJobEntry.xul";
  }
}
