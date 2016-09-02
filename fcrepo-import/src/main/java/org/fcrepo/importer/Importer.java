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
package org.fcrepo.importer;

import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.TransferProcess;
import org.slf4j.Logger;

/**
 * Fedora Import Utility
 *
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public class Importer implements TransferProcess {
    private static final Logger logger = getLogger(Importer.class);
    private Config config;
    protected FcrepoClient client;

    private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final String REPOSITORY_NAMESPACE = "http://fedora.info/definitions/v4/repository#";
    private static final String PREMIS_SIZE = "http://www.loc.gov/premis/rdf/v1#hasSize";
    private static final String PREMIS_DIGEST = "http://www.loc.gov/premis/rdf/v1#hasMessageDigest";
    private static final String LDP_CONTAINS = "http://www.w3.org/ns/ldp#contains";

    /**
     * Constructor that takes the Import/Export configuration
     *
     * @param config for import
     */
    public Importer(final Config config) {
        this.config = config;
        this.client = FcrepoClient.client().build();
    }

    /**
     * This method does the import
     */
    public void run() {
        System.out.println("Importing!");
        importDirectory(config.getDescriptionDirectory());
    }

    private void importDirectory(final File dir) {
        for (final File f : dir.listFiles()) {
            if (f.isDirectory()) {
                importDirectory(f);
            } else if (f.isFile()) {
                importFile(f);
            }
        }
    }

    private void importFile(final File f) {
        try {
            logger.info("Importing {}", f.getAbsolutePath());
            final URI uri = uriForFile(f, config.getDescriptionDirectory());
            System.out.println("Importing: " + f.getAbsolutePath() + " => " + uri);
            putResource(uri, f);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void putResource(final URI uri, final File f) throws IOException {
        FcrepoResponse response = null;
        try {
            response = client.put(uri).body(sanitizedTriples(f, uri), config.getRdfLanguage()).perform();
        } catch (FcrepoOperationFailedException ex) {
            System.out.println("Exception while importing " + uri.toString() + ": " + ex.toString());
        }
        if (response.getStatusCode() > 204) {
            System.out.println("Imported " + f.getAbsolutePath() + " to " + uri.toString());
        } else {
            System.out.println("Error while importing " + uri.toString() + ": " + IOUtils.toString(response.getBody()));
        }
    }

    private InputStream sanitizedTriples(final File f, final URI uri) throws IOException {
        final Model model = createDefaultModel().read(
                new FileInputStream(f), null, config.getRdfLanguage());
        final List<Statement> remove = new ArrayList<>();
        for (final StmtIterator it = model.listStatements(); it.hasNext(); ) {
            final Statement s = it.nextStatement();

            if (s.getPredicate().getNameSpace().equals(REPOSITORY_NAMESPACE)
                    || s.getPredicate().getURI().equals(LDP_CONTAINS)
                    || s.getPredicate().getURI().equals(PREMIS_DIGEST)
                    || s.getPredicate().getURI().equals(PREMIS_SIZE)
                    || s.getSubject().getURI().endsWith("fcr:export?format=jcr/xml")
                    || (s.getPredicate().getURI().equals(RDF_TYPE)
                        && s.getResource().getNameSpace().equals(REPOSITORY_NAMESPACE)) ) {
                remove.add(s);
            }
        }
        model.remove(remove);
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        model.write(buf, config.getRdfLanguage());
        return new ByteArrayInputStream(buf.toByteArray());
    }

    private URI uriForFile(final File f, final File baseDir) throws URISyntaxException {
        String relative = baseDir.toPath().relativize(f.toPath()).toString();
        if (relative.startsWith("rest") && config.getResource().toString().endsWith("/rest")) {
            relative = relative.substring("rest".length());
        }
        if (relative.endsWith(config.getRdfExtension())) {
            relative = relative.substring(0, relative.length() - config.getRdfExtension().length());
        }

        // TODO parse RDF to figure out the real URI?
        if (relative.endsWith("/fcr_metadata")) {
            relative = relative.substring(0, relative.length() - "fcr_metadata".length()) + "/fcr:metadata";
        }
        return new URI(config.getResource() + relative);
    }
}
