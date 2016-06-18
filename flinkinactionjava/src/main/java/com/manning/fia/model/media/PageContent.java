package com.manning.fia.model.media;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by hari on 6/6/16.
 */
@SuppressWarnings("serial")
public class PageContent implements Serializable{

    //pageId
    private long pageId;

    //contentId
    private long id;

    //title of the page
    private String title;

    //contents
    private byte[] contents;

    //active
    private boolean activeFlag;

    private String[] parent_keywords;


    public PageContent() {
    }


    public PageContent(long pageId, long id, String title, byte[] contents, boolean activeFlag, String[] parent_keywords) {
        this.pageId = pageId;
        this.id = id;
        this.title = title;
        this.contents = contents;
        this.activeFlag = activeFlag;
        this.parent_keywords = parent_keywords;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public byte[] getContents() {
        return contents;
    }

    public void setContents(byte[] contents) {
        this.contents = contents;
    }

    public boolean isActiveFlag() {
        return activeFlag;
    }

    public void setActiveFlag(boolean activeFlag) {
        this.activeFlag = activeFlag;
    }

    public String[] getParent_keywords() {
        return parent_keywords;
    }

    public void setParent_keywords(String[] parent_keywords) {
        this.parent_keywords = parent_keywords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageContent)) return false;

        PageContent that = (PageContent) o;

        if (pageId != that.pageId) return false;
        if (id != that.id) return false;
        if (activeFlag != that.activeFlag) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (!Arrays.equals(contents, that.contents)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parent_keywords, that.parent_keywords);

    }

    @Override
    public int hashCode() {
        int result = (int) (pageId ^ (pageId >>> 32));
        result = 31 * result + (int) (id ^ (id >>> 32));
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(contents);
        result = 31 * result + (activeFlag ? 1 : 0);
        result = 31 * result + Arrays.hashCode(parent_keywords);
        return result;
    }

    @Override
    public String toString() {
        return "PageContent{" +
                "pageId=" + pageId +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", contents=" + Arrays.toString(contents) +
                ", activeFlag=" + activeFlag +
                ", parent_keywords=" + Arrays.toString(parent_keywords) +
                '}';
    }
}
