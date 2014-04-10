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
 * --------------------
 * (c) 2002-2013 by Mathias Lux (mathias@juggle.at)
 *  http://www.semanticmetadata.net/lire, http://www.lire-project.net
 *
 * Updated: 15.04.13 15:20
 */

package net.semanticmetadata.lire.indexing.parallel;


/**
 * This file is part of LIRE, a Java library for content based image retrieval.
 *
 * @author Mathias Lux, mathias@juggle.at, 15.04.13
 */

public class WorkItem implements ImageInfo {

	private String id;
	
    private String fileName;
    
    private String title;
    private String location;
    private String lng;
    private String lat;

	private String tags;
    
    private byte[] buffer;
    
    public WorkItem() {}
    
    public WorkItem(String fileName, String title, String location,
			String tags, byte[] buffer) {
		super();
		this.fileName = fileName;
		this.title = title;
		this.location = location;
		this.tags = tags;
		this.buffer = buffer;
	}
    
    public WorkItem(ImageInfo item) {
    	this.id = item.getId();
		this.fileName = item.getFileName();
		this.title = item.getTitle();
		this.location = item.getLocation();
		this.lat = item.getLat();
		this.lng = item.getLng();
		this.tags = item.getTags();
		this.buffer = item.getBuffer();
    }
    
    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLng() {
		return lng;
	}

	public void setLng(String lng) {
		this.lng = lng;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}
}
