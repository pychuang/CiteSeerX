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

import org.apache.solr.client.solrj.impl.CloudSolrServer;

public class DocumentCloudIndexUpdater extends DocumentIndexUpdater {
    private String zkHosts;
    private String solrCollection;

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }

    public void setSolrCollection(String solrCollection) {
        this.solrCollection = solrCollection;
    }

    @Override
    protected void prepare () throws Exception {
        CloudSolrServer server = new CloudSolrServer(zkHosts);

        System.out.println("Set default Solr collection: " + solrCollection);
        server.setDefaultCollection(solrCollection);
        solrServer = server;
        super.prepare();
    }
}
