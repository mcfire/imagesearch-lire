package net.semanticmetadata.lire.impl.docbuilder;

import java.awt.image.BufferedImage;

import net.semanticmetadata.lire.AbstractDocumentBuilder;
import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.indexing.parallel.ImageInfo;
import net.semanticmetadata.lire.utils.DocumentUtils;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class TextDocumentBuilder extends AbstractDocumentBuilder {

    /**
     * Creates a new, empty ChainedDocumentBuilder.
     */
    public TextDocumentBuilder() {
    }

    public void addBuilder(DocumentBuilder builder) {
    }

    @Override
    public Field[] createDescriptorFields(BufferedImage image) {
        return new Field[]{};
    }

    public Document createDocument(BufferedImage image, ImageInfo imageInfo) {
        Document doc = new Document();
        
        DocumentUtils.appendImageInfoFields(doc, imageInfo);
        return doc;
    }
}
