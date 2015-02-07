/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.AdapterApp;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.cdap.test.internal.AppFabricClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTests extends AppFabricTestBase {
  private static LocationFactory locationFactory;
  private static File adapterDir;
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration conf = getInjector().getInstance(CConfiguration.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    adapterDir = new File(conf.get(Constants.AppFabric.ADAPTER_DIR));
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerAdapters();
  }

  @Test
  public void testAdapters() throws Exception {
    //Basic adapter service tests.
    String namespaceId = Constants.DEFAULT_NAMESPACE;

    ImmutableMap<String, String> properties = ImmutableMap.of("frequency", "1m");
    ImmutableMap<String, String> sourceProperties = ImmutableMap.of();
    ImmutableMap<String, String> sinkProperties = ImmutableMap.of("dataset.class", FileSet.class.getName());

    String adapterName = "myAdapter";
    AdapterSpecification adapterSpecification =
      new AdapterSpecification(adapterName, "dummyAdapter", properties,
                               ImmutableSet.of(new Source("mySource", Source.Type.STREAM, sourceProperties)),
                               ImmutableSet.of(new Sink("mySink", Sink.Type.DATASET, sinkProperties)));

    // Create Adapter
    adapterService.createAdapter(namespaceId, adapterSpecification);
    PreferencesStore preferencesStore = getInjector().getInstance(PreferencesStore.class);
    Map<String, String> prop = preferencesStore.getResolvedProperties(namespaceId, adapterSpecification.getType());
    Assert.assertTrue(Boolean.parseBoolean(prop.get(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED)));
    try {
      // Expect another call to create Adapter with the same adapterName to throw an AdapterAlreadyExistsException.
      adapterService.createAdapter(namespaceId, adapterSpecification);
      Assert.fail("Second call to create adapter with same adapterName did not throw AdapterAlreadyExistsException.");
    } catch (AdapterAlreadyExistsException expected) {
    }

    AdapterSpecification actualAdapterSpec = adapterService.getAdapter(namespaceId, adapterName);
    Assert.assertNotNull(actualAdapterSpec);
    Assert.assertEquals(adapterSpecification, actualAdapterSpec);

    // list all adapters
    Collection<AdapterSpecification> adapters = adapterService.getAdapters(namespaceId);
    Assert.assertArrayEquals(new AdapterSpecification[] {adapterSpecification}, adapters.toArray());

    // Delete Adapter
    adapterService.removeAdapter(namespaceId, "myAdapter");
    // verify that the adapter is deleted
    try {
      adapterService.getAdapter(namespaceId, adapterName);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapterName));
    } catch (AdapterNotFoundException expected) {
    }

    adapters = adapterService.getAdapters(namespaceId);
    Assert.assertTrue(adapters.isEmpty());
  }

  @Test
  public void testInvalidJars() throws Exception {
    Class<?> clz = AdapterApp.class;
    String adapterType = "adapterType";

    Attributes attributes = generateRequiredAttributes(clz, adapterType);
    setupAdapterJarWithManifestAttributes(clz, attributes);

    // Using a valid manifest (no missing attributes) results in the adapterTypeInfo being registered
    adapterService.registerAdapters();
    Assert.assertNotNull(adapterService.getAdapterTypeInfo(adapterType));

    // removing the any of the required attributes from the manifest results in the AdapterTypeInfo not being created.
    // Missing the CDAP-Source-Type attribute
    adapterType = "adapterType1";
    attributes = new Attributes();
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerAdapters();
    Assert.assertNull(adapterService.getAdapterTypeInfo(adapterType));

    // Missing the CDAP-Sink-Type attribute
    adapterType = "adapterType2";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerAdapters();
    Assert.assertNull(adapterService.getAdapterTypeInfo(adapterType));

    // Missing the CDAP-Adapter-Type attribute
    adapterType = "adapterType3";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerAdapters();
    Assert.assertNull(adapterService.getAdapterTypeInfo(adapterType));

    // Missing the CDAP-Adapter-Program-Type attribute
    adapterType = "adapterType4";
    attributes = new Attributes();
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    setupAdapterJarWithManifestAttributes(clz, attributes);

    adapterService.registerAdapters();
    Assert.assertNull(adapterService.getAdapterTypeInfo(adapterType));
  }

  private static Attributes generateRequiredAttributes(Class<?> clz, String adapterType) {
    Attributes attributes = new Attributes();
    attributes.put(ManifestFields.MAIN_CLASS, clz.getName());
    attributes.put(ManifestFields.MANIFEST_VERSION, "1.0");
    attributes.putValue("CDAP-Source-Type", "STREAM");
    attributes.putValue("CDAP-Sink-Type", "DATASET");
    attributes.putValue("CDAP-Adapter-Type", adapterType);
    attributes.putValue("CDAP-Adapter-Program-Type", ProgramType.WORKFLOW.toString());
    return attributes;
  }


  private void setupAdapterJarWithManifestAttributes(Class<?> clz, Attributes attributes) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);
    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(AdapterApp.class, "dummyAdapter");
  }

  private static void setupAdapter(Class<?> clz, String adapterType) throws IOException {
    Attributes attributes = generateRequiredAttributes(clz, adapterType);

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);

    File adapterJar = AppFabricClient.createDeploymentJar(locationFactory, clz, manifest);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(adapterJar, destination);
  }
}
