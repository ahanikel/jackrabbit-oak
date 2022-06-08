/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.segment.azure.persistentcache;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE)
public class PersistentAzureCacheService {

    private static final Logger log = LoggerFactory.getLogger(PersistentAzureCacheService.class);

    private ServiceRegistration serviceRegistration;
    private AbstractPersistentCache azureCache;

    @Activate
    public void activate(ComponentContext context, PersistentAzureCacheConfiguration configuration) {
        if (configuration.enabled()) {
            try {
                final CloudStorageAccount azure = CloudStorageAccount.parse(createAzureConnectionURLFrom(configuration));
                final CloudBlobContainer container = azure.createCloudBlobClient().getContainerReference(configuration.containerName());
                final CloudBlobDirectory azureDirectory = container.getDirectoryReference(configuration.rootPath());
                azureCache = new PersistentAzureCache(azureDirectory);
                final Properties props = new Properties();
                props.put("backend", "azure");
                serviceRegistration = context.getBundleContext().registerService(PersistentCache.class.getName(), azureCache, props);
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

    @Deactivate
    public void deactivate() {
        if (serviceRegistration != null) {
            serviceRegistration.unregister();
            serviceRegistration = null;
        }
        if (azureCache != null) {
            azureCache.close();
            azureCache = null;
        }
    }

    private static String createAzureConnectionURLFrom(PersistentAzureCacheConfiguration configuration) {
        if (!StringUtils.isBlank(configuration.connectionURL())) {
            return configuration.connectionURL();
        }
        if (!StringUtils.isBlank(configuration.sharedAccessSignature())) {
            return createAzureConnectionURLFromSasUri(configuration);
        }
        return createAzureConnectionURLFromAccessKey(configuration);
    }

    private static String createAzureConnectionURLFromAccessKey(PersistentAzureCacheConfiguration configuration) {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(configuration.accountName()).append(';');
        connectionString.append("AccountKey=").append(configuration.accessKey()).append(';');
        if (!StringUtils.isBlank(configuration.blobEndpoint())) {
            connectionString.append("BlobEndpoint=").append(configuration.blobEndpoint()).append(';');
        }
        return connectionString.toString();
    }

    private static String createAzureConnectionURLFromSasUri(PersistentAzureCacheConfiguration configuration) {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(configuration.accountName()).append(';');
        connectionString.append("SharedAccessSignature=").append(configuration.sharedAccessSignature()).append(';');
        if (!StringUtils.isBlank(configuration.blobEndpoint())) {
            connectionString.append("BlobEndpoint=").append(configuration.blobEndpoint()).append(';');
        }
        return connectionString.toString();
    }
}