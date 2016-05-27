package com.manning.fia.model.media;


import org.apache.flink.api.java.tuple.Tuple2;

public class CommentsPlatform {

    private long eventId;

    // pageId and pageTitle
    private Tuple2<Long,String> page;

    // the user commenting about the article
    private ApplicationUser applicationUser;

    private String comments;


    public CommentsPlatform(long eventId, Tuple2<Long, String> page, ApplicationUser applicationUser, String comments) {
        this.eventId = eventId;
        this.page = page;
        this.applicationUser = applicationUser;
        this.comments = comments;
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public Tuple2<Long, String> getPage() {
        return page;
    }

    public void setPage(Tuple2<Long, String> page) {
        this.page = page;
    }

    public ApplicationUser getApplicationUser() {
        return applicationUser;
    }

    public void setApplicationUser(ApplicationUser applicationUser) {
        this.applicationUser = applicationUser;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommentsPlatform)) return false;

        CommentsPlatform that = (CommentsPlatform) o;

        if (eventId != that.eventId) return false;
        if (page != null ? !page.equals(that.page) : that.page != null) return false;
        if (applicationUser != null ? !applicationUser.equals(that.applicationUser) : that.applicationUser != null)
            return false;
        return comments != null ? comments.equals(that.comments) : that.comments == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + (page != null ? page.hashCode() : 0);
        result = 31 * result + (applicationUser != null ? applicationUser.hashCode() : 0);
        result = 31 * result + (comments != null ? comments.hashCode() : 0);
        return result;
    }
}
