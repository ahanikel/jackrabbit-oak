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

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(
        pid = {PersistentAzureCacheConfiguration.PID},
        name = "Apache Jackrabbit Oak Azure Persistent Cache Service",
        description = "Persistent cache for the Oak Segment Node Store")
public @interface PersistentAzureCacheConfiguration {

    String PID = "org.apache.jackrabbit.oak.segment.azure.persistentcache.PersistentAzureCacheService";

    @AttributeDefinition(
            name = "Azure cache",
            description = "Boolean value indicating that the azure-persisted cache should be used for segment store"
    )
    boolean azureCacheEnabled() default false;

    @AttributeDefinition(
            name = "Azure cache connection string",
            description = "Azure cache connection string"
    )
    String azureCacheConnectionString() default "";

    @AttributeDefinition(
            name = "Azure cache container name",
            description = "Azure cache container name"
    )
    String azureCacheContainer();

    @AttributeDefinition(
            name = "Azure cache container directory name",
            description = "Azure cache container directory name"
    )
    String azureCacheDirectory() default "aem-cache";
}