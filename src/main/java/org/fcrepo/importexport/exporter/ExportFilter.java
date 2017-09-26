/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.importexport.exporter;

import java.net.URI;
import java.util.List;

/**
 * @author Mike Durbin
 */
public interface ExportFilter {

    /**
     * Check whether the specified binary resource should be included in the export or filtered out.
     * @param descriptionUri URIs for RDF descriptions of the given binary resource
     * @param binaryUrl the URI for the binary resource
     * @return true if the resource should be included, false if it should be omitted from the export
     */
    boolean includeBinaryResource(List<URI> descriptionUri, URI binaryUrl);

    /**
     * Check whether the specified RDF resource should be include din the export or filtered out.
     * @param resourceUri the URI of the RDF resource
     * @return true if the resource should be included, false if it shoudl be omitted from the export
     */
    boolean includeRDFResource(URI resourceUri);
}
