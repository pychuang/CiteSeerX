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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.base.CharMatcher;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

import edu.psu.citeseerx.domain.ThinDoc;

abstract public class IndexUpdater {
    private ExecutorService threadPool;
    protected SolrServer solrServer;
    protected final int indexBatchSize = 1000;

    {
        int cpus = Runtime.getRuntime().availableProcessors();

        threadPool = Executors.newFixedThreadPool(cpus * 2);
    }

    public void update() throws Exception {
        prepare();
        index();
        processDeletions();
        cleanup();
    }

    abstract protected void prepare() throws Exception;
    abstract protected List<ThinDoc> retrieveNextBatchOfDocuments();

    private void index() throws Exception {
        int counter = 0;

        while (true) {
            List<ThinDoc> docs = retrieveNextBatchOfDocuments();
            if (docs.isEmpty()) {
                break;
            }

            counter += indexDocuments(docs);
            solrServer.commit();
            System.out.println(counter + " documents added");
        }

    }

    private int indexDocuments(List<ThinDoc> docs) {
        ArrayList<Future> futures = new ArrayList<Future>();

        for (ThinDoc doc : docs) {
            futures.add(threadPool.submit(new TaskIndexDocument(doc)));
        }

        try {
            for (Future f : futures) {
                f.get();
                System.out.print('.');
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println();

        return docs.size();
    }

    abstract protected void prepareIndexDocument(ThinDoc doc);

    private class TaskIndexDocument implements Callable<Void> {
        private final ThinDoc document;

        public TaskIndexDocument(ThinDoc doc) {
            prepareIndexDocument(doc);
            document = doc;
        }

        public Void call() throws Exception {
            SolrInputDocument solrDoc = buildSolrInputDocument(document);
            solrServer.add(solrDoc);
            System.out.print('.');
            return null;
        }
    }

    abstract protected SolrInputDocument buildSolrInputDocument(ThinDoc doc) throws Exception;

    protected void processDeletions() throws Exception {
    }

    protected void cleanup() throws Exception {
        System.out.println("shutdown thread pool");
        threadPool.shutdown();

        System.out.println("optimize...");
        solrServer.optimize();
        System.out.println("shutdown solr server...");
        solrServer.shutdown();
        System.out.println("solr server shutdown");
    }
}
