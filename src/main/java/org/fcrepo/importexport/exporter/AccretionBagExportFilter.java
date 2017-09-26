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

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.codec.binary.Hex.encodeHex;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A filter that endeavors to filter out binary and RDF resources that are known to be identical to the
 * version in a previously created bagit bag.  This filter is guranteed to retain files that are changed
 * from previous versions.  This strategy is ideal for systems (like AP Trust) where bags can be submitted
 * including just additions/changes which are applied to previously submitted versions.
 *
 * Binary resources are considered identical if they have the same SHA-1 hash.
 *
 * Non-RDF resource are considered identical if their serialization has the same SHA-1 hash.  While
 * fedora implementations make no guarantees about producing bit-identical RDF serializations, in
 * practice many do.  In the even that the serialization produces a different SHA-1 but it semantically
 * identical, this filter will fail to filter it out.  This is considered acceptable since the
 * only guarantee this filter makes is to ensure that changed resources are included.
 *
 * @author Mike Durbin
 */
public class AccretionBagExportFilter implements ExportFilter {

    private static final Logger logger = getLogger(AccretionBagExportFilter.class);

    private Map<String, String> pathToSha1;

    private MessageDigest sha1;

    private FcrepoClient client;

    private Config config;

    /**
     * Constructs an AccretionBagExportFilter for the given SHA-1 manifest.
     * @param sha1Manifest a bagit manifest for a bag whose entries will be filtered by this filter if
     *                     unchanged.
     * @param client a fedora client, used to ascertain if resources are unchanged
     * @param config a config used in generating the current bag
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public AccretionBagExportFilter(final File sha1Manifest, final FcrepoClient client, final Config config)
            throws IOException, NoSuchAlgorithmException {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(sha1Manifest)))) {
            this.pathToSha1 = new HashMap<String, String>();
            final Pattern p = Pattern.compile("([^\\s]+)\\s+data([^\\s].*)");
            String line = null;
            while ((line = r.readLine()) != null) {
                final Matcher m = p.matcher(line);
                if (m.matches()) {
                    pathToSha1.put(m.group(2).trim(), m.group(1).trim());
                } else {
                    throw new RuntimeException("Unable to parse line from " + sha1Manifest.getAbsolutePath()
                            + ": " + line);
                }
            }
        }
        this.client = client;
        this.config = config;
        sha1 = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public boolean includeBinaryResource(final List<URI> descriptionUri, final URI binaryUri) {
        final String path = TransferProcess.encodePath(binaryUri.getPath()) + BINARY_EXTENSION;
        final String oldSha1 = pathToSha1.get(path);
        logger.info("SHA-1 from previous bag for " + path + " is " + oldSha1);

        if (descriptionUri == null || descriptionUri.isEmpty()) {
            // we don't know the sha1 of the current file, so include it
            return true;
        } else {
            for (URI d : descriptionUri) {
                final String currentSha1 = getReportedSha1ForRDFResource(d, client);
                if (currentSha1 != null) {
                    return !currentSha1.equalsIgnoreCase(oldSha1);
                }
            }
        }
        return true;
    }

    @Override
    public boolean includeRDFResource(final URI resourceUri) {
        final String path = TransferProcess.encodePath(resourceUri.getPath()) + config.getRdfExtension();
        final String oldSha1 = pathToSha1.get(path);
        logger.info("SHA-1 from previous bag for " + path + " is " + oldSha1);
        try {
            final GetBuilder getBuilder = client.get(resourceUri).accept(config.getRdfLanguage());
            try (FcrepoResponse response = getBuilder.perform()) {
                final InputStream is = response.getBody();
                int read = 0;
                final byte[] buf = new byte[8192];
                while ((read = is.read(buf)) != -1) {
                    if (sha1 != null) {
                        sha1.update(buf, 0, read);
                    }
                }
                final String currentSha1 = new String(encodeHex(sha1.digest()));
                return !currentSha1.equalsIgnoreCase(oldSha1);
            } catch (FcrepoOperationFailedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FcrepoOperationFailedException e) {

        }

        return true;
    }

    private static String getReportedSha1ForRDFResource(final URI uri, final FcrepoClient client) {
        final String lang = "application/n-triples";
        try {
            final GetBuilder getBuilder = client.get(uri).accept(lang);
            try (FcrepoResponse response = getBuilder.perform()) {
                final Model model = createDefaultModel().read(response.getBody(), null, lang);
                final NodeIterator it = model.listObjectsOfProperty(
                        createProperty("http://www.loc.gov/premis/rdf/v1#hasMessageDigest"));
                while (it.hasNext()) {
                    final RDFNode n = it.next();
                    final String value = n.asNode().getURI().toString();
                    if (value.startsWith("urn:sha1:")) {
                        return value.substring(9);
                    }
                }
            }
        } catch (IOException | FcrepoOperationFailedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }


}
