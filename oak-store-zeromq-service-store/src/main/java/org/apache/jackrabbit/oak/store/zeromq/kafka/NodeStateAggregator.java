/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq.kafka;

import java.util.ArrayList;
import java.util.List;

public class NodeStateAggregator {
    /*
    n^ root
    n: bcc37746-ea77-724e-b585-92e7da302ece e69af743-6741-4a78-8d3b-8893a2d1f685
    n^ jcr:system
    n: 399e10ca-3fd9-218c-356e-63e8a761d7cf 665fd063-9c24-49db-b24a-a5bd70b5dd2b
    n^ rep:namespaces
    n: 5b843d85-a0de-1336-dfdd-9e27bc40506a 074b33a7-b1fb-457d-8bcd-b31697b3d7e3
    p+ p <STRING> = n
    n^ rep:nsdata
    n: db69e013-666b-f577-76a8-d3917cdce8f2 778318ac-d634-45ad-a435-436b46dabd7e
    p^ rep:prefixes <STRINGS> = [,p,sv,xml,nt,jcr,oak,rep,mix]
    p^ rep:uris <STRINGS> = [,http://jackrabbit.apache.org/oak/ns/1.0,internal,http://www.jcp.org/jcr/sv/1.0,http://www.jcp.org/jcr/mix/1.0,http://www.jcp.org/jcr/1.0,http://www.w3.org/XML/1998/namespace,http://www.jcp.org/jcr/nt/1.0,n]
    p+ n <STRING> = p
    n!
    n!
    n!
    n!
    n^ root
    n: 15230b3a-8146-bfd3-9930-afdb4edf059f bcc37746-ea77-724e-b585-92e7da302ece
    n^ jcr:system
    n: 186c8562-594e-a188-0202-1c07b544a2b6 399e10ca-3fd9-218c-356e-63e8a761d7cf
    n^ rep:namespaces
    n: bf06a16c-2ad3-96a8-18ca-4dbb1109e453 5b843d85-a0de-1336-dfdd-9e27bc40506a
    p+ p2 <STRING> = n2
    n^ rep:nsdata
    n: 555db196-7774-d3ce-2fce-4951140ed73b
    p^ rep:prefixes <STRINGS> = [,p,p2,sv,xml,nt,jcr,oak,rep,mix]
    p^ rep:uris <STRINGS> = [,http://jackrabbit.apache.org/oak/ns/1.0,internal,http://www.jcp.org/jcr/sv/1.0,n2,http://www.jcp.org/jcr/mix/1.0,http://www.jcp.org/jcr/1.0,http://www.w3.org/XML/1998/namespace,http://www.jcp.org/jcr/nt/1.0,n]
    p+ n2 <STRING> = p2
    n!
    n!
    n!
    n!
     */

    private int level;
    private final List<String> paths;
    private final List<String> uuids;
    private final List<String> parentUuids;
    private final List<String> ops;
    private final List<String> deletedChildNodes;
    private final List<String> propertyChanges;

    public NodeStateAggregator() {
        level = 0;
        paths = new ArrayList<>();
        uuids = new ArrayList<>();
        parentUuids = new ArrayList<>();
        ops = new ArrayList<>();
        deletedChildNodes = new ArrayList<>();
        propertyChanges = new ArrayList<>();
    }

    public boolean handle(String op, String val) {
        switch (op) {
            case "n^": {
                ++level;
                paths.add(val);
                ops.add(op);
                return false;
            }
            case "n:": {
                final int firstSpace = val.indexOf(' ');
                uuids.add(val.substring(0, firstSpace));
                parentUuids.add(val.substring(firstSpace + 1));
                return false;
            }
            case "n+": {
                ++level;
                paths.add(val);
                ops.add(op);
                return false;
            }
            case "n-": {
                deletedChildNodes.add(val);
                return false;
            }
            case "p+":
            case "p^":
            case "p-": {
                propertyChanges.add(val);
                return false;
            }
            case "n!": {
                if (--level < 0) {
                    throw new IllegalStateException("Root already committed");
                }
                return true;
            }
            default:
                throw new IllegalArgumentException("Unknown operator: " + op);
        }
    }
}
