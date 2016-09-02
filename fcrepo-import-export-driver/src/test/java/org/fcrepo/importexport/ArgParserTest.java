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
package org.fcrepo.importexport;

import org.fcrepo.exporter.Exporter;
import org.fcrepo.importer.Importer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author awoods
 * @since 2016-08-29
 */
public class ArgParserTest {

    private ArgParser parser;

    @Before
    public void setUp() throws Exception {
        parser = new org.fcrepo.importexport.driver.ArgParser();
    }

    @Test
    public void parseValidImport() throws Exception {
        final String[] args = new String[]{"-m", "import",
                                           "-d", "/tmp/rdf",
                                           "-r", "http://localhost:8080/rest/1"};
        final TransferProcess processor = parser.parse(args);
        Assert.assertTrue(processor instanceof Importer);
    }

    @Test
    public void parseValidExport() throws Exception {
        final String[] args = new String[]{"-m", "export",
                "-d", "/tmp/rdf",
                "-b", "/tmp/bin",
                "-x", ".jsonld",
                "-l", "application/ld+json",
                "-r", "http://localhost:8080/rest/1"};
        final TransferProcess processor = parser.parse(args);
        Assert.assertTrue(processor instanceof Exporter);
    }

    @Test (expected = RuntimeException.class)
    public void parseInvalid() throws Exception {
        final String[] args = new String[]{"junk"};
        parser.parse(args);
    }

}
