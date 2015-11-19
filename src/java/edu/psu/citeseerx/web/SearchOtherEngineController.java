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
package edu.psu.citeseerx.web;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;
import org.springframework.web.servlet.view.RedirectView;

import edu.psu.citeseerx.webutils.RedirectUtils;

/**
 * Process a request to redirect to an associated URL for a given query. If
 * for some reason the associated URL is not found an error is generated.
 * @author Po-Yu Chuang
 * @version $Rev$ $Date$
 */
public class SearchOtherEngineController implements Controller {

    private final static String GOOGLE = "https://www.google.com/search";
    private final static String GS = "http://scholar.google.com/scholar";
    private final static String BING = "http://www.bing.com/search";
    private final static String MAS = "http://academic.research.microsoft.com/Search.aspx";
    private final static String S2 = "http://s2.allenai.org/search";
    private final static String DBLP = "http://dblp.uni-trier.de/search";

    /* (non-Javadoc)
     * @see org.springframework.web.servlet.mvc.Controller#handleRequest(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    public ModelAndView handleRequest(HttpServletRequest request,
            HttpServletResponse response) throws Exception {

        String searchEngine = request.getParameter("site");
        String query = request.getParameter("q");

        String url = null;

        System.err.println("site:"+searchEngine);
        System.err.println("query:"+query);
        if ("google".equalsIgnoreCase(searchEngine)) {
            url = GOOGLE + "?q=" + query;
        } else if ("gs".equalsIgnoreCase(searchEngine)) {
            url = GS + "?q=" + query;
        } else if ("bing".equalsIgnoreCase(searchEngine)) {
            url = BING + "?q=" + query;
        } else if ("mas".equalsIgnoreCase(searchEngine)) {
            url = MAS + "?query=" + query;
        } else if ("s2".equalsIgnoreCase(searchEngine)) {
            url = S2 + "?q=" + query;
        } else if ("dblp".equalsIgnoreCase(searchEngine)) {
            url = DBLP + "?q=" + query;
        } else {
            return new ModelAndView(new RedirectView("search"), "q", query);
        }

        System.out.println("redirect to:"+url);
        RedirectUtils.externalRedirect(response, url);

        return null;
    }
}
