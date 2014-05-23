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
 * Updated: 16.07.13 15:44
 */

package net.semanticmetadata.lire.indexing.parallel;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import javax.imageio.ImageIO;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.DocumentBuilderFactory;
import net.semanticmetadata.lire.impl.docbuilder.ChainedDocumentBuilder;
import net.semanticmetadata.lire.impl.docbuilder.TextDocumentBuilder;
import net.semanticmetadata.lire.indexing.LireCustomCodec;
import net.semanticmetadata.lire.utils.DocumentUtils;
import net.semanticmetadata.lire.utils.LuceneUtils;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * This class allows for creating indexes in a parallel manner. The class
 * at hand reads files from the disk and acts as producer, while several consumer
 * threads extract the features from the given files.
 *
 * @author Mathias Lux, mathias@juggle.at, 15.04.13
 */

public class ParallelIndexer implements Runnable {
    private int numberOfThreads = 10;
    private String indexPath;
    private String imageDirectory;
    Stack<WorkItem> images = new Stack<WorkItem>();
    IndexWriter writer;
    boolean ended = false;
    boolean threadFinished = false;
    Iterator<ImageInfo> imageInfos;
    int overallCount = 0;
    private IndexWriterConfig.OpenMode openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
    // all xx seconds a status message will be displayed
    private int monitoringInterval = 30;
    
    private String imageDataPath;
    
    /**
     * 提取创建文档索引的核心代码，用于在论文中解释
     * @param imageInfos
     * 将要被建立索引的图像信息列表
     * @throws IOException
     */
    public void indexImageTextFeature(List<ImageInfo> imageInfos) throws IOException {
    	//创建Lucene索引配置，使用IKAnalyzer为中文分词器
        IndexWriterConfig config = new IndexWriterConfig(LuceneUtils.LUCENE_VERSION, new IKAnalyzer());
        //设置配置中的索引打开模式为创建或追加
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        //使用Lucene索引配置和指定的索引位置，创建索引器。
        IndexWriter writer = new IndexWriter(FSDirectory.open(new File(indexPath)), config);
        
        //对于每一个图像信息，将其文字描述和地理位置信息建立索引
        for (ImageInfo imageInfo : imageInfos) {
	        Document doc = new Document();
	        //将图像文件的标题作为文档ID
	    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_IDENTIFIER, imageInfo.getTitle(), Field.Store.YES));
	    	//为文档标题建立索引，TextField类型的文本将会分词后存储
	    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_TITLE, imageInfo.getTitle(), Field.Store.YES));
	    	//保存图像信息数据库中的ID
	    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_DBID, imageInfo.getId(), Field.Store.YES));
	    	//保存图像内容中事物的地理位置信息
	    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_LNG, imageInfo.getLng(), Field.Store.YES));
	    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_LAT, imageInfo.getLat(), Field.Store.YES));
	    	//写入Lucene索引
	    	writer.addDocument(doc);
        }
   	   	//关闭索引器
    	writer.close();
    }

    /**
     * @param numberOfThreads
     * @param indexPath
     * @param imageDirectory  a directory containing all the images somewhere in the child hierarchy.
     */
    public ParallelIndexer(int numberOfThreads, String imageDataPath, String indexPath, Iterator<ImageInfo> imageInfos) {
        this.numberOfThreads = numberOfThreads;
        this.imageDataPath =imageDataPath;
        this.indexPath = indexPath;
        this.imageInfos = imageInfos;
    }

    /**
     * Overwrite this method to define the builders to be used within the Indexer.
     *
     * @param builder
     */
    public void addBuilders(TextDocumentBuilder builder) {
        builder.addBuilder(DocumentBuilderFactory.getCEDDDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getFCTHDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getJCDDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getPHOGDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getOpponentHistogramDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getJointHistogramDocumentBuilder());
//        builder.addBuilder(DocumentBuilderFactory.getAutoColorCorrelogramDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getColorLayoutBuilder());
        builder.addBuilder(DocumentBuilderFactory.getEdgeHistogramBuilder());
//        builder.addBuilder(DocumentBuilderFactory.getScalableColorBuilder());
//        builder.addBuilder(DocumentBuilderFactory.getLuminanceLayoutDocumentBuilder());
//        builder.addBuilder(DocumentBuilderFactory.getColorHistogramDocumentBuilder());
    }

    public void run() {
        IndexWriterConfig config = new IndexWriterConfig(LuceneUtils.LUCENE_VERSION, new IKAnalyzer());
        config.setOpenMode(openMode);
        config.setCodec(new LireCustomCodec());
        try {
            if (imageDirectory != null) System.out.println("Getting all images in " + imageDirectory + ".");
            writer = new IndexWriter(FSDirectory.open(new File(indexPath)), config);
            
            Thread p = new Thread(new Producer());
            p.start();
            LinkedList<Thread> threads = new LinkedList<Thread>();
            long l = System.currentTimeMillis();
            for (int i = 0; i < numberOfThreads; i++) {
                Thread c = new Thread(new Consumer());
                c.start();
                threads.add(c);
            }
            Thread m = new Thread(new Monitoring());
            m.start();
            for (Iterator<Thread> iterator = threads.iterator(); iterator.hasNext(); ) {
                iterator.next().join();
            }
            long l1 = System.currentTimeMillis() - l;
            System.out.println("Analyzed " + overallCount + " images in " + l1 / 1000 + " seconds, ~" + ((overallCount>0)?(l1 / overallCount):"n.a.") + " ms each.");
            writer.commit();
            writer.close();
            threadFinished = true;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check is this thread is still running.
     *
     * @return
     */
    public boolean hasEnded() {
        return threadFinished;
    }

    class Monitoring implements Runnable {
        public void run() {
            long ms = System.currentTimeMillis();
            try {
                Thread.sleep(1000 * monitoringInterval); // wait xx seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!ended) {
                try {
                    // print the current status:
                    long time = System.currentTimeMillis() - ms;
                    System.out.println("Analyzed " + overallCount + " images in " + time / 1000 + " seconds, " + ((overallCount>0)?(time / overallCount):"n.a.") + " ms each ("+images.size()+" images currently in queue).");
                    Thread.sleep(1000 * monitoringInterval); // wait xx seconds
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class Producer implements Runnable {
        public void run() {
        	
            int tmpSize = 0;
            while (imageInfos.hasNext()) {
            	ImageInfo item = imageInfos.next();
            	
            	String filePath = imageDataPath + File.separator + item.getFileName();
                File imageFile = new File(filePath);
                try {
                    int fileSize = (int) imageFile.length();
                    byte[] buffer = new byte[fileSize];
                    FileInputStream fis = new FileInputStream(filePath);
                    fis.read(buffer);
                    fis.close();
                    
                    WorkItem newItem = new WorkItem(item);
                    newItem.setBuffer(buffer);
                    synchronized (images) {
                        // this helps a lot for slow computers ....
                        if (images.size()>500) images.wait(5000);
                        images.add(newItem);
                        tmpSize = images.size();
                        images.notifyAll();
                    }
                    try {
                        // it's actually hard to manage the amount of memory used to cache images.
                        // On faster computers it turns out to be good to have a big cache, on
                        // slower ones the cache poses a serious problem and leads to memory and GC exceptions.
                        // iy you encounter still memory errors, then try to use more threads.
                        if (tmpSize > 500) Thread.sleep(50);
                        else if (tmpSize > 1000) Thread.sleep(5000);
                        else if (tmpSize > 2000) Thread.sleep(50000);
                        else if (tmpSize > 3000) Thread.sleep(500000);
                        else Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    System.err.println("Could not open " + item.getFileName() + ". " + e.getMessage());
//                    e.printStackTrace();
                }
            }
            synchronized (images) {
                ended = true;
                images.notifyAll();
            }
        }
    }

    /**
     * Consumers take the images prepared from the Producer and extract all the image features.
     */
    class Consumer implements Runnable {
        WorkItem tmp = null;
        TextDocumentBuilder builder = new TextDocumentBuilder();
        int count = 0;
        boolean locallyEnded = false;

        Consumer() {
            addBuilders(builder);
        }

        public void run() {
            while (!locallyEnded) {
                synchronized (images) {
                    // we wait for the stack to be either filled or empty & not being filled any more.
                    while (images.empty() && !ended) {
                        try {
                            images.wait(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    // make sure the thread locally knows that the end has come (outer loop)
                    if (images.empty() && ended)
                        locallyEnded = true;
                    // well the last thing we want is an exception in the very last round.
                    if (!images.empty() && !locallyEnded) {
                        tmp = images.pop();
                        count++;
                        overallCount++;
                    }
                }
                try {
                    if (!locallyEnded) {
                    	//TODO
//                        ByteArrayInputStream b = new ByteArrayInputStream(tmp.getBuffer());
//                        BufferedImage img = ImageIO.read(b);
                        Document d = builder.createDocument((BufferedImage)null, tmp);
                        writer.addDocument(d);
                    }
                } catch (Exception e) {
                    System.err.println("[ParallelIndexer] Could not handle file " + tmp.getFileName() + ": "  + e.getMessage());
                    e.printStackTrace();
                }
            }
//            System.out.println("Images analyzed: " + count);
        }
    }

	public String getImageDataPath() {
		return imageDataPath;
	}
}
