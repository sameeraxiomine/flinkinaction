package com.manning.fia.model.media;

import java.io.Serializable;

/**
 * Created by hari on 6/5/16.
 */
@SuppressWarnings("serial")
public class Page implements Serializable{

    // pageId
    private long id;

    // author writing the article
    private String author;

    // url of the page
    private String url;

    // title of the page
    private String title;

    // content type .ie to see if it is article,blog etc.
    private String contentType;

    // date of the article so that it will be partioned by date.
    private long publishDate;

    //section
    private String section;

    //subsection
    private String subSection;

    //topic
    private String topic;


    public Page() {
    }


    public Page(long id, String author, String url, String description, String contentType, long publishDate, String section, String subSection, String topic) {
        this.id = id;
        this.author = author;
        this.url = url;
        this.title = description;
        this.contentType = contentType;
        this.publishDate = publishDate;
        this.section = section;
        this.subSection = subSection;
        this.topic = topic;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getPublishDate() {
        return publishDate;
    }

    public void setPublishDate(long publishDate) {
        this.publishDate = publishDate;
    }

    public String getSection() {
        return section;
    }

    public void setSection(String section) {
        this.section = section;
    }

    public String getSubSection() {
        return subSection;
    }

    public void setSubSection(String subSection) {
        this.subSection = subSection;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Page)) return false;

        Page that = (Page) o;

        if (id != that.id) return false;
        if (publishDate != that.publishDate) return false;
        if (author != null ? !author.equals(that.author) : that.author != null) return false;
        if (url != null ? !url.equals(that.url) : that.url != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (contentType != null ? !contentType.equals(that.contentType) : that.contentType != null) return false;
        if (section != null ? !section.equals(that.section) : that.section != null) return false;
        if (subSection != null ? !subSection.equals(that.subSection) : that.subSection != null) return false;
        return topic != null ? topic.equals(that.topic) : that.topic == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (author != null ? author.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (contentType != null ? contentType.hashCode() : 0);
        result = 31 * result + (int) (publishDate ^ (publishDate >>> 32));
        result = 31 * result + (section != null ? section.hashCode() : 0);
        result = 31 * result + (subSection != null ? subSection.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PageInformation{" +
                "id=" + id +
                ", author='" + author + '\'' +
                ", url='" + url + '\'' +
                ", description='" + title + '\'' +
                ", contentType='" + contentType + '\'' +
                ", publishDate=" + publishDate +
                ", section='" + section + '\'' +
                ", subSection='" + subSection + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
