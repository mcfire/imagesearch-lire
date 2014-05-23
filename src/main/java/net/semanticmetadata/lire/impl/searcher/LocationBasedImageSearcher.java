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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.semanticmetadata.lire.AbstractImageSearcher;
import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.ImageDuplicates;
import net.semanticmetadata.lire.ImageSearchHits;
import net.semanticmetadata.lire.impl.SimpleImageSearchHits;
import net.semanticmetadata.lire.impl.SimpleResult;
import net.semanticmetadata.lire.indexing.parallel.ImageInfo;
import net.semanticmetadata.lire.indexing.parallel.WorkItem;
import net.semanticmetadata.lire.utils.GeoUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.Bits;

/**
 * @author mc
 *
 */
public class LocationBasedImageSearcher extends AbstractImageSearcher {
    protected Logger logger = Logger.getLogger(getClass().getName());

    //最大检索结果数量
    private int maxHits = 20;
    //检索结果列表
    protected List<SimpleResult> docs;
    //最大距离阀值
    private float threshold = 3;

    public LocationBasedImageSearcher(int maxHits) {
        this.maxHits = maxHits;
        docs = new ArrayList<SimpleResult>();
    }
    
    public ImageSearchHits search(BufferedImage image, ImageInfo imageInfo, IndexReader reader) throws IOException {
        
    	if (StringUtils.isEmpty(imageInfo.getLng()) || StringUtils.isEmpty(imageInfo.getLat())) return null;
    	
    	//查找距离最近的文档，并返回最近文档的距离值
        float maxDistance = findSimilar(reader, imageInfo);
        //构建检索结果并返回
        SimpleImageSearchHits searchHits = new SimpleImageSearchHits(this.docs, maxDistance);
        return searchHits;
    }

    /**
     * @param reader
     * @param lireFeature
     * @return the maximum distance found for normalizing.
     * @throws java.io.IOException
     */
    protected float findSimilar(IndexReader reader, ImageInfo imageInfo) throws IOException {
        float maxDistance = -1f, allMaxDistance = -1f;
        float tmpDistance = 0f;

        docs.clear();
        //找出当前可用的文档列表，有些文档可能已经被删除
        Bits liveDocs = MultiFields.getLiveDocs(reader);

        int docNumber = reader.numDocs();
        Document d = null;
        for (int i = 0; i < docNumber; i++) {
            if (reader.hasDeletions() && !liveDocs.get(i)) continue; //如果文档已经被删除，则忽略此文档

            d = reader.document(i);//读取文档
            tmpDistance = getDistance(d, imageInfo);//计算文档与检索对象之间的距离
            //若距离大于阀值，则视为无效距离。不作为检索特征。
            if (tmpDistance < 0 || tmpDistance > this.threshold) continue;

            //根据当前距离设置全局最大距离
            if (allMaxDistance < tmpDistance) {
                allMaxDistance = tmpDistance;
            }
            //当前是第一个文档时设置最大距离
            if (maxDistance < 0) {
                maxDistance = tmpDistance;
            }
            //当结果数量没有达到指定数量时，向结果中添加当前文档
            if (this.docs.size() < maxHits) {
                this.docs.add(new SimpleResult(tmpDistance, d, i));                
                if (tmpDistance > maxDistance) maxDistance = tmpDistance;
            //当结果数量大于指定数量，并且当前距离比结果中最大距离更近时
            } else if (tmpDistance < maxDistance) {
            	//将结果中最距离最远的文档替换为当前文档
                this.docs.remove(this.docs.size() - 1);
                this.docs.add(new SimpleResult(tmpDistance, d, i));
                //更新最大距离
                maxDistance = tmpDistance;

                Collections.sort(docs);
            }
        }
        //将最大距离返回
        return maxDistance;
    }

    //计算文档和待检索图像之间的地理位置距离
    protected float getDistance(Document doc, ImageInfo imageInfo) {

        //从文档中提取地理位置信息
        String latText = doc.get(DocumentBuilder.FIELD_NAME_LAT);
        String lngText = doc.get(DocumentBuilder.FIELD_NAME_LNG);
        
        if (StringUtils.isEmpty(latText) || StringUtils.isEmpty(lngText)
        	|| StringUtils.isEmpty(imageInfo.getLat()) || StringUtils.isEmpty(imageInfo.getLng())) {
        	return -1;
        }

        //将文档中的地理位置信息转换为经纬度，浮点型数值
        double lat = Double.parseDouble(latText);
        double lng = Double.parseDouble(lngText);
        
      //将待检索图像中的地理位置信息转换为经纬度，浮点型数值
    	double latQuery = Double.parseDouble(imageInfo.getLat());
    	double lngQuery = Double.parseDouble(imageInfo.getLng());
        
    	if (Math.abs(lat - latQuery) >= 1 || Math.abs(lng - lngQuery) >= 1) {
    		return Integer.MAX_VALUE;
    	}
        
        if (logger.isLoggable(Level.INFO)) {
        	logger.info("calcute distance:(" + latText + ", " + lngText
        			+ ") and (" + imageInfo.getLat() + ", " + imageInfo.getLng() + ")");
        }
    	
        //计算并返回两点之间的距离
        return (float)GeoUtils.getGeoDistance(lat, lng, latQuery, lngQuery);
    }

    public ImageDuplicates findDuplicates(IndexReader reader) throws IOException {
    	throw new UnsupportedOperationException();
    }

    public ImageSearchHits search(Document doc, IndexReader reader) throws IOException {
    	WorkItem imageInfo = new WorkItem();
    	imageInfo.setLat(doc.get(DocumentBuilder.FIELD_NAME_LAT));
    	imageInfo.setLng(doc.get(DocumentBuilder.FIELD_NAME_LNG));
    	
    	BufferedImage image = null;
        return this.search(image, imageInfo, reader);
    }

    public ImageSearchHits search(BufferedImage image, IndexReader reader) throws IOException {
    	return this.search(image, null, reader);
    }

    public String toString() {
        return "Location based searcher";
    }

}
