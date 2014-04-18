package net.semanticmetadata.lire.utils;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.indexing.parallel.ImageInfo;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

public class DocumentUtils {

    
    public static void appendImageInfoFields(Document doc, ImageInfo imageInfo) {
    	if (imageInfo == null) return;
    	
    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_IDENTIFIER, imageInfo.getTitle(), Field.Store.YES));
    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_DBID, imageInfo.getId(), Field.Store.YES));
    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_TITLE, imageInfo.getTitle(), Field.Store.YES));
    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_TAGS, imageInfo.getTags(), Field.Store.YES));
    	doc.add(new TextField(DocumentBuilder.FIELD_NAME_LOCATION, imageInfo.getLocation(), Field.Store.YES));
    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_LNG, imageInfo.getLng(), Field.Store.YES));
    	doc.add(new StringField(DocumentBuilder.FIELD_NAME_LAT, imageInfo.getLat(), Field.Store.YES));
    }
}
