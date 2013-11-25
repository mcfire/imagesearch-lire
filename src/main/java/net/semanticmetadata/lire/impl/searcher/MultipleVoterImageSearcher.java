/*
 * This file is part of the LIRE project: http://www.semanticmetadata.net/lire
 * LIRE is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * LIRE is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LIRE; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * We kindly ask you to refer the any or one of the following publications in
 * any publication mentioning or employing Lire:
 *
 * Lux Mathias, Savvas A. Chatzichristofis. Lire: Lucene Image Retrieval –
 * An Extensible Java CBIR Library. In proceedings of the 16th ACM International
 * Conference on Multimedia, pp. 1085-1088, Vancouver, Canada, 2008
 * URL: http://doi.acm.org/10.1145/1459359.1459577
 *
 * Lux Mathias. Content Based Image Retrieval with LIRE. In proceedings of the
 * 19th ACM International Conference on Multimedia, pp. 735-738, Scottsdale,
 * Arizona, USA, 2011
 * URL: http://dl.acm.org/citation.cfm?id=2072432
 *
 * Mathias Lux, Oge Marques. Visual Information Retrieval using Java and LIRE
 * Morgan & Claypool, 2013
 * URL: http://www.morganclaypool.com/doi/abs/10.2200/S00468ED1V01Y201301ICR025
 *
 * Copyright statement:
 * ====================
 * (c) 2002-2013 by Mathias Lux (mathias@juggle.at)
 *  http://www.semanticmetadata.net/lire, http://www.lire-project.net
 *
 * Updated: 11.07.13 10:51
 */
package net.semanticmetadata.lire.impl.searcher;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import net.semanticmetadata.lire.AbstractImageSearcher;
import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.ImageDuplicates;
import net.semanticmetadata.lire.ImageSearchHits;
import net.semanticmetadata.lire.ImageSearcher;
import net.semanticmetadata.lire.impl.SimpleImageSearchHits;
import net.semanticmetadata.lire.impl.SimpleResult;
import net.semanticmetadata.lire.indexing.parallel.ImageInfo;
import net.semanticmetadata.lire.indexing.parallel.WorkItem;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

/**
 * @author mc
 *
 */
public class MultipleVoterImageSearcher extends AbstractImageSearcher {
    protected Logger logger = Logger.getLogger(getClass().getName());
    
    private List<ImageSearcher> searchers;

    private int maxHits = 10;
    protected Map<String, SimpleResult> docs;

    public MultipleVoterImageSearcher(int maxHits, List<ImageSearcher> searchers) {
        this.maxHits = maxHits;
        this.searchers = searchers;
        
        docs = new TreeMap<String, SimpleResult>();
    }
    
    /**
     * zero based position
     * @param position
     * @return
     */
    protected float getPositionWeight(int position) {
    	if (position < 0) {
    		return 0;
    	}
    	return 3 / (position + 1);
    }

    public ImageSearchHits search(Document doc, IndexReader reader) throws IOException {
    	WorkItem imageInfo = new WorkItem(null, doc.get(DocumentBuilder.FIELD_NAME_TITLE), 
										null, DocumentBuilder.FIELD_NAME_TAGS, null);
    	imageInfo.setLat(doc.get(DocumentBuilder.FIELD_NAME_LAT));
    	imageInfo.setLng(doc.get(DocumentBuilder.FIELD_NAME_LNG));
    	
    	BufferedImage image = null;
        return this.search(image, imageInfo, reader);
    }

    public ImageSearchHits search(BufferedImage image, IndexReader reader) throws IOException {
    	return this.search(image, null, reader);
    }
    
    public ImageSearchHits search(BufferedImage image, ImageInfo imageInfo, IndexReader reader) throws IOException {
        logger.finer("Starting extraction.");
        float maxWeight = 0;
        SimpleImageSearchHits searchHits = null;
        
        for (ImageSearcher searcher : searchers) {
        	ImageSearchHits result = searcher.search(image, imageInfo, reader);
        	if (result == null || result.length() == 0) continue;
        	
        	for (int i = 0; i < result.length(); i++) {
        		Document doc = result.doc(i);
        		String id = doc.get(DocumentBuilder.FIELD_NAME_IDENTIFIER);
        		SimpleResult record = new SimpleResult(1, doc, i);
        		
        		if (docs.containsKey(id)) {
        			record = docs.get(id);
        		} else {
        			docs.put(id, record);
        		}
    			
    			float weight = record.getDistance();
    			weight = weight + (this.getPositionWeight(i) * searcher.getWeight());
    			
    			record.setDistance(weight);
        	}
        }
        
        List<SimpleResult> sortedDocs = new ArrayList<SimpleResult>(docs.values());
        Collections.sort(sortedDocs, new Comparator<SimpleResult> () {
			@Override
			public int compare(SimpleResult o1, SimpleResult o2) {
				return Float.compare(o2.getDistance(), o1.getDistance());
			}
        });
        
        List<SimpleResult> resultDocs;
        if (sortedDocs.size() > 0) {
        	resultDocs = sortedDocs.subList(0, Math.min(maxHits, sortedDocs.size()));
        	maxWeight = resultDocs.get(resultDocs.size() - 1).getDistance();
        } else {
        	resultDocs = new ArrayList<SimpleResult>(0);
        }
        
        searchHits = new SimpleImageSearchHits(resultDocs, maxWeight);
        return searchHits;
    }

    /**
     * @param reader
     * @param lireFeature
     * @return the maximum distance found for normalizing.
     * @throws java.io.IOException
     */
    protected void findSimilar(ImageInfo imageInfo) throws IOException {
        
    }

    protected float getDistance(Document d, ImageInfo imageInfo) {

        Double lng = null, lat = null;
        
        String lngText = d.get(DocumentBuilder.FIELD_NAME_LNG);
        String latText = d.get(DocumentBuilder.FIELD_NAME_LAT);
        
        try {
        	lng = Double.parseDouble(lngText);
        	lat = Double.parseDouble(latText);
        } catch (Exception e) {}
        
        if (lng == null && lat == null) return 0;
        
        Double lngQuery = null, latQuery = null;
        
        String lngQueryText = d.get(DocumentBuilder.FIELD_NAME_LNG);
        String latQueryText = d.get(DocumentBuilder.FIELD_NAME_LAT);
        
        try {
        	lngQuery = Double.parseDouble(lngQueryText);
        	latQuery = Double.parseDouble(latQueryText);
        } catch (Exception e) {}
        
        if (lngQuery == null && latQuery == null) return 0;
        
        return (float)Math.sqrt((Math.pow(lngQuery - lng, 2d) + Math.pow(latQuery - lat, 2d)));
    }

    public ImageDuplicates findDuplicates(IndexReader reader) throws IOException {
    	throw new UnsupportedOperationException();
    }

    public String toString() {
        return "Location based searcher";
    }

}
