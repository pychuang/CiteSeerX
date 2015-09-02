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

import java.lang.Exception;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.base.CharMatcher;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;

import edu.psu.citeseerx.dao2.logic.CSXDAO;
import edu.psu.citeseerx.dao2.logic.CiteClusterDAO;
import edu.psu.citeseerx.domain.Author;
import edu.psu.citeseerx.domain.Document;
import edu.psu.citeseerx.domain.DocumentFileInfo;
import edu.psu.citeseerx.domain.DomainTransformer;
import edu.psu.citeseerx.domain.Keyword;
import edu.psu.citeseerx.domain.ThinDoc;
import edu.psu.citeseerx.utility.SafeText;

abstract public class DocumentIndexUpdater extends IndexUpdater {
    private CSXDAO csxdao;
    private CiteClusterDAO citedao;
    private Date currentTime = new Date(System.currentTimeMillis());
    private Date lastUpdate;
    private long lastIndexedCluster = 0;

    public void setCSXDAO(CSXDAO csxdao) {
        this.csxdao = csxdao;
    }

    public void setCiteClusterDAO(CiteClusterDAO citedao) {
        this.citedao = citedao;
    }

    @Override
    protected void prepare() throws Exception {
        boolean reindex = false;

        if (reindex) {
            lastUpdate = new Date((long)0);
        } else {
            lastUpdate = citedao.getLastIndexTime();
        }
    }

    @Override
    protected void processDeletions() throws Exception {
        System.out.println("process deletion...");
        List<Long> list = citedao.getDeletions(currentTime);
        for (Long id : list) {
            solrServer.deleteById(id.toString());
        }

        solrServer.commit();
        citedao.removeDeletions(currentTime);
    }

    @Override
    protected void cleanup() throws Exception {
        citedao.setLastIndexTime(currentTime);
        super.cleanup();
    }

    @Override
    protected List<ThinDoc> retrieveNextBatchOfDocuments() {
        System.out.println("lastIndexedCluster=" + lastIndexedCluster);
        return citedao.getClustersSinceTime(lastUpdate, new Long(lastIndexedCluster), indexBatchSize);
    }

    @Override
    protected void prepareIndexDocument(ThinDoc doc) {
        lastIndexedCluster = doc.getCluster();
    }

    @Override
    protected SolrInputDocument buildSolrInputDocument(ThinDoc doc) throws Exception {
        Long clusterid = doc.getCluster();
        List<Long> cites = new ArrayList<Long>();
        List<Long> citedby = new ArrayList<Long>();

        cites = citedao.getCitedClusters(clusterid);
        citedby = citedao.getCitingClusters(clusterid);

        if (doc.getInCollection() == false) {
            // We don't have the full document. Index the citation
            return buildSolrInputDocument(doc, cites, citedby);
        }

        Document fullDoc = findFullDocument(clusterid);
        if (fullDoc == null) {
            // The full document it's not public. Index the citation
            return buildSolrInputDocument(doc, cites, citedby);
        }

        // Index the full document
        fullDoc.setClusterID(clusterid);
        fullDoc.setNcites(doc.getNcites());
        return buildSolrInputDocument(fullDoc, cites, citedby);
    }

    private Document findFullDocument(Long clusterid) {
        List<String> dois = citedao.getPaperIDs(clusterid);

        for (String doi : dois) {
            Document fullDoc = csxdao.getDocumentFromDB(doi, false, false);

            if (fullDoc != null && fullDoc.isPublic()) {
                return fullDoc;
            }
        }

        return null;
    }

    /**
     * Builds a record in Solr update syntax corresponding to the
     * supplied parameters, and adds it to the supplied element
     * @param doc
     * @param cites
     * @param citedby
     */
    private SolrInputDocument buildSolrInputDocument(Document doc, List<Long> cites,
            List<Long> citedby) throws Exception {
        String id = doc.getClusterID().toString();
        String doi = doc.getDatum(Document.DOI_KEY, Document.ENCODED);
        String title = doc.getDatum(Document.TITLE_KEY, Document.ENCODED);
        String venue = doc.getDatum(Document.VENUE_KEY, Document.ENCODED);
        String year = doc.getDatum(Document.YEAR_KEY, Document.ENCODED);
        String abs = doc.getDatum(Document.ABSTRACT_KEY, Document.ENCODED);
        String text = getText(doc);
        long vtime = (doc.getVersionTime() != null) ? doc.getVersionTime().getTime() : 0;
        int ncites = doc.getNcites();
        int scites = doc.getSelfCites();

        List<Keyword> keys = doc.getKeywords();
        ArrayList<String> keywords = new ArrayList<String>();
        for (Keyword key : keys) {
            keywords.add(key.getDatum(Keyword.KEYWORD_KEY, Keyword.ENCODED));
        }

        List<Author> authors = doc.getAuthors();
        ArrayList<String> authorNames = new ArrayList<String>();
        for (Author author  : authors) {
            String name = author.getDatum(Author.NAME_KEY, Author.ENCODED);
            if (name != null) {
                authorNames.add(name);
            }
        }

        List<String> authorNorms = buildAuthorNorms(authorNames);

        StringBuffer citesBuffer = new StringBuffer();
        for (Iterator<Long> cids = cites.iterator(); cids.hasNext(); ) {
            citesBuffer.append(cids.next());
            if (cids.hasNext()) {
                citesBuffer.append(" ");
            }
        }

        StringBuffer citedbyBuffer = new StringBuffer();
        for (Iterator<Long> cids = citedby.iterator(); cids.hasNext(); ) {
            citedbyBuffer.append(cids.next());
            if (cids.hasNext()) {
                citedbyBuffer.append(" ");
            }
        }

        SolrInputDocument solrDoc = new SolrInputDocument();

        solrDoc.addField("id", id);
        if (doi != null) {
            solrDoc.addField("doi", doi);
            solrDoc.addField("incol", "1");
        } else {
            solrDoc.addField("incol", "0");
        }

        if (title != null) {
            solrDoc.addField("title", title);
        }

        if (venue != null) {
            solrDoc.addField("venue", venue);
        }

        if (abs != null) {
            solrDoc.addField("abstract", abs);
        }

        solrDoc.addField("ncites", Integer.toString(ncites));
        solrDoc.addField("scites", Integer.toString(scites));

        try {
            int year_i = Integer.parseInt(year);
            solrDoc.addField("year", Integer.toString(year_i));
        } catch (Exception e) { }

        for (String keyword : keywords) {
            solrDoc.addField("keyword", keyword);
        }

        for (String name : authorNames) {
            solrDoc.addField("author", name);
        }

        for (String norm : authorNorms) {
            solrDoc.addField("authorNorms", norm);
        }

        if (text != null) {
            solrDoc.addField("text", text);
        }

        solrDoc.addField("cites", citesBuffer.toString());
        solrDoc.addField("citedby", citedbyBuffer.toString());
        solrDoc.addField("vtime", Long.toString(vtime));

        return solrDoc;
    } //- buildSolrInputDocument

    /**
     * Translates the supplied ThinDoc to a Document object and passes
     * control the the Document-based buildSolrInputDocument method.
     * @param thinDoc
     * @param cites
     * @param citedby
     */
    private SolrInputDocument buildSolrInputDocument(ThinDoc thinDoc, List<Long> cites,
            List<Long> citedby) throws Exception {
        Document doc = DomainTransformer.toDocument(thinDoc);
        return buildSolrInputDocument(doc, cites, citedby);
    }  //- buildSolrInputDocument

    /**
     * Builds a list of author normalizations to create more flexible
     * author search.
     * @param names
     * @return
     */
    private static List<String> buildAuthorNorms(List<String> names) {
        HashSet<String> norms = new HashSet<String>();
        for (String name : names) {
            name = name.replaceAll("[^\\p{L} ]", "");
            StringTokenizer st = new StringTokenizer(name);
            String[] tokens = new String[st.countTokens()];
            int counter = 0;
            while(st.hasMoreTokens()) {
                tokens[counter] = st.nextToken();
                counter++;
            }
            norms.add(joinStringArray(tokens));

            if (tokens.length > 2) {

                String[] n1 = new String[tokens.length];
                for (int i=0; i<tokens.length; i++) {
                    if (i<tokens.length-1) {
                        n1[i] = Character.toString(tokens[i].charAt(0));
                    } else {
                        n1[i] = tokens[i];
                    }
                }

                String[] n2 = new String[tokens.length];
                for (int i=0; i<tokens.length; i++) {
                    if (i>0 && i<tokens.length-1) {
                        n2[i] = Character.toString(tokens[i].charAt(0));
                    } else {
                        n2[i] = tokens[i];
                    }
                }

                norms.add(joinStringArray(n1));
                norms.add(joinStringArray(n2));
            }

            if (tokens.length > 1) {

                String[] n3 = new String[2];
                n3[0] = tokens[0];
                n3[1] = tokens[tokens.length-1];

                String[] n4 = new String[2];
                n4[0] = Character.toString(tokens[0].charAt(0));
                n4[1] = tokens[tokens.length-1];

                norms.add(joinStringArray(n3));
                norms.add(joinStringArray(n4));
            }
        }

        ArrayList<String> normList = new ArrayList<String>();
        for (Iterator<String> it = norms.iterator(); it.hasNext(); ) {
            normList.add(it.next());
        }

        return normList;
    }  //- buildAuthorNorms

    private static String joinStringArray(String[] strings) {
        StringBuffer buffer = new StringBuffer();
        for (int i=0; i<strings.length; i++) {
            buffer.append(strings[i]);
            if (i<strings.length-1) {
                buffer.append(" ");
            }
        }

        return buffer.toString();
    }  //- joinStringArray

    /**
     * Fetches the full text of a document from the filesystem repository.
     * @param doc
     * @return
     */
    private String getText(Document doc) throws Exception {
        String doi = doc.getDatum(Document.DOI_KEY);
        if (doi == null) {
            return null;
        }

        String repID = doc.getFileInfo().getDatum(DocumentFileInfo.REP_ID_KEY);
        FileInputStream ins;
        try {
            try {
                ins = csxdao.getFileInputStream(doi, repID, "body");
            } catch (IOException e) {
                ins = csxdao.getFileInputStream(doi, repID, "txt");
            }
        } catch (IOException e) {
            return null;
        }

        String text = IOUtils.toString(ins, "UTF-8");
        text = SafeText.stripBadChars(text);
        text = CharMatcher.JAVA_ISO_CONTROL.replaceFrom(text, " ");
        try { ins.close(); } catch (IOException e) { }
        return text;
    }  //- getText
}
