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
        
        //遍历自身包含的每个检索器
        for (ImageSearcher searcher : searchers) {
        	//调用检索器进行检索
        	ImageSearchHits result = searcher.search(image, imageInfo, reader);
        	if (result == null || result.length() == 0) continue;
        	
        	//对检索到的文档进行处理
        	for (int i = 0; i < result.length(); i++) {
        		Document doc = result.doc(i);
        		//读取文档ID
        		String id = doc.get(DocumentBuilder.FIELD_NAME_DBID);
        		SimpleResult record = new SimpleResult(result.score(i), doc, i);
        		
        		//如果当前结果列表中已包含文档，则返回文档，否则将当前文档加入结果列表
        		if (docs.containsKey(id)) {
        			record = docs.get(id);
        		} else {
        			docs.put(id, record);
        		}
    			
        		//根据文档顺序和检索器权重，计算文档权重
    			float weight = 1f - record.getDistance();
    			weight = weight * searcher.getWeight() * this.getPositionWeight(i);
    			
    			//将文档的权重进行更新
    			record.setDistance(record.getDistance() + weight);
        	}
        }
        
        //根据更新后的文档权重，对文档进行排序
        List<SimpleResult> sortedDocs = new ArrayList<SimpleResult>(docs.values());
        Collections.sort(sortedDocs, Collections.reverseOrder());
        
        //截取前N文档作为综合检索结果
        List<SimpleResult> resultDocs;
        if (sortedDocs.size() > 0) {
        	resultDocs = sortedDocs.subList(0, Math.min(maxHits, sortedDocs.size()));
        	maxWeight = resultDocs.get(resultDocs.size() - 1).getDistance();
        } else {
        	resultDocs = new ArrayList<SimpleResult>(0);
        }
        
        //构建并返回检索结果
        searchHits = new SimpleImageSearchHits(resultDocs, maxWeight);
        return searchHits;
    }
    
    /**
     * zero based position
     * @param position
     * @return
     */
    protected float getPositionWeight(float position) {
    	if (position < 0) {
    		return 0f;
    	}
    	return 3f / (position + 1f);
    }

    public ImageDuplicates findDuplicates(IndexReader reader) throws IOException {
    	throw new UnsupportedOperationException();
    }

    public String toString() {
        return "Location based searcher";
    }

}
