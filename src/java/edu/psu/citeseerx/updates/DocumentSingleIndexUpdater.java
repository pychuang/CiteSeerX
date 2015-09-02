/*
 * Copyright 2015 Penn State University
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.psu.citeseerx.updates;

import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;

public class DocumentSingleIndexUpdater extends DocumentIndexUpdater {
    private String solrUpdateUrl;

    public void setSolrURL(String solrUpdateUrl) {
        this.solrUpdateUrl = solrUpdateUrl;
    }

    @Override
    protected void prepare () throws Exception {
        int cpus = Runtime.getRuntime().availableProcessors();

        solrServer = new ConcurrentUpdateSolrServer(solrUpdateUrl, indexBatchSize, cpus);
        super.prepare();
    }
}
