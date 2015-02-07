/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.utils.ApplicationBundler;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * {@link co.cask.cdap.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
public class RemoteDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetFramework.class);

  private final DatasetServiceClient client;
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final DatasetTypeClassLoaderFactory typeLoader;

  @Inject
  public RemoteDatasetFramework(DiscoveryServiceClient discoveryClient,
                                DatasetDefinitionRegistryFactory registryFactory,
                                DatasetTypeClassLoaderFactory typeLoader) {

    this.client = new DatasetServiceClient(discoveryClient);
    this.registryFactory = registryFactory;
    this.typeLoader = typeLoader;
  }

  @Override
  public void addModule(String moduleName, DatasetModule module)
    throws DatasetManagementException {

    // We support easier APIs for custom datasets: user can implement dataset and make it available for others to use
    // by only implementing Dataset. Without requiring implementing datasets module, definition and other classes.
    // In this case we wrap that Dataset implementation with SingleTypeModule. But since we don't have a way to serde
    // dataset modules, if we pass only SingleTypeModule.class the Dataset implementation info will be lost. Hence, as
    // a workaround we put Dataset implementation class in MDS (on DatasetService) and wrapping it with SingleTypeModule
    // when we need to instantiate module.
    //
    // todo: do proper serde for modules instead of just passing class name to server
    Class<?> typeClass;
    if (module instanceof SingleTypeModule) {
      typeClass = ((SingleTypeModule) module).getDataSetClass();
    } else {
      typeClass = module.getClass();
    }

    addModule(moduleName, typeClass);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    client.deleteModule(moduleName);
  }

  @Override
  public void deleteAllModules() throws DatasetManagementException {
    client.deleteModules();
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException {

    client.addInstance(datasetInstanceName, datasetType, props);
  }

  @Override
  public void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException {
    client.updateInstance(datasetInstanceName, props);
  }

  @Override
  public Collection<DatasetSpecification> getInstances() throws DatasetManagementException {
    return client.getAllInstances();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    DatasetMeta meta = client.getInstance(name);
    return meta == null ? null : meta.getSpec();
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return client.getInstance(instanceName) != null;
  }

  @Override
  public boolean hasType(String typeName) throws DatasetManagementException {
    return client.getType(typeName) != null;
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    client.deleteInstance(datasetInstanceName);
  }

  @Override
  public void deleteAllInstances() throws DatasetManagementException, IOException {
    // delete all one by one
    for (DatasetSpecification spec : getInstances()) {
      deleteInstance(spec.getName());
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    DatasetMeta instanceInfo = client.getInstance(datasetInstanceName);
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), classLoader);
    return (T) type.getAdmin(instanceInfo.getSpec());
  }

  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName, Map<String, String> arguments,
                                          ClassLoader classLoader) throws DatasetManagementException, IOException {

    DatasetMeta instanceInfo = client.getInstance(datasetInstanceName);
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), classLoader);
    return (T) type.getDataset(instanceInfo.getSpec(), arguments);
  }

  private void addModule(String moduleName, Class<?> typeClass) throws DatasetManagementException {
    try {
      File tempFile = File.createTempFile(typeClass.getName(), ".jar");
      try {
        Location tempJarPath = createDeploymentJar(typeClass, new LocalLocationFactory().create(tempFile.toURI()));
        client.addModule(moduleName, typeClass.getName(), tempJarPath);
      } finally {
        tempFile.delete();
      }
    } catch (IOException e) {
      String msg = String.format("Could not create jar for deploying dataset module %s with main class %s",
                                 moduleName, typeClass.getName());
      LOG.error(msg, e);
      throw new DatasetManagementException(msg, e);
    }
  }

  private static Location createDeploymentJar(Class<?> clz, Location destination) throws IOException {
    Location tempBundle = destination.getTempFile(".jar");
    try {
      ClassLoader remembered = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(clz.getClassLoader());
      try {
        ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.cdap.api",
                                                                             "org.apache.hadoop",
                                                                             "org.apache.hbase",
                                                                             "org.apache.hive"));
        bundler.createBundle(tempBundle, clz);
      } finally {
        Thread.currentThread().setContextClassLoader(remembered);
      }

      // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
      // by the ApplicationBundler inside Twill.
      JarOutputStream jarOutput = new JarOutputStream(destination.getOutputStream());
      try {
        JarInputStream jarInput = new JarInputStream(tempBundle.getInputStream());
        try {
          Set<String> seen = Sets.newHashSet();
          JarEntry jarEntry = jarInput.getNextJarEntry();
          while (jarEntry != null) {
            boolean isDir = jarEntry.isDirectory();
            String entryName = jarEntry.getName();
            if (!entryName.equals("classes/")) {
              if (entryName.startsWith("classes/")) {
                jarEntry = new JarEntry(entryName.substring("classes/".length()));
              } else {
                jarEntry = new JarEntry(entryName);
              }
              if (seen.add(jarEntry.getName())) {
                jarOutput.putNextEntry(jarEntry);

                if (!isDir) {
                  ByteStreams.copy(jarInput, jarOutput);
                }
              }
            }

            jarEntry = jarInput.getNextJarEntry();
          }
        } finally {
          jarInput.close();
        }

      } finally {
        jarOutput.close();
      }

      return destination;
    } finally {
      tempBundle.delete();
    }
  }

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  public <T extends DatasetType> T getDatasetType(DatasetTypeMeta implementationInfo,
                                                  ClassLoader classLoader)
    throws DatasetManagementException {


    if (classLoader == null) {
      classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    }

    DatasetDefinitionRegistry registry = registryFactory.create();
    List<DatasetModuleMeta> modulesToLoad = implementationInfo.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      // adding dataset module jar to classloader
      try {
        classLoader = typeLoader.create(moduleMeta, classLoader);
      } catch (IOException e) {
        LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                  moduleMeta, implementationInfo, e);
        throw Throwables.propagate(e);
      }

      Class<?> moduleClass;

      // try program class loader then cdap class loader
      try {
        moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
      } catch (ClassNotFoundException e) {
        try {
          moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), null, this);
        } catch (ClassNotFoundException e2) {
          LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                    moduleMeta.getClassName(), implementationInfo, e);
          throw Throwables.propagate(e);
        }
      }

      try {
        DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
        module.register(registry);
      } catch (Exception e) {
        LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                  moduleMeta.getClassName(), implementationInfo, e);
        throw Throwables.propagate(e);
      }
    }

    return (T) new DatasetType(registry.get(implementationInfo.getName()), classLoader);
  }
}
