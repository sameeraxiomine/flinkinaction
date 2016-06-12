package com.manning.fia.model.media;

public class ApplicationUser {

    // uniqueidentifer based on cookie
    private String uuid;

    // actual subscriber registered in the media company
    private String subscriberId;

    private String ipAddress;


    public ApplicationUser() {
    }

    public ApplicationUser(String uuid, String subscriberId, String ipAddress) {
        this.uuid = uuid;
        this.subscriberId = subscriberId;
        this.ipAddress = ipAddress;
    }

    public ApplicationUser(String uuid, String ipAddress) {
        this.uuid = uuid;
        this.ipAddress = ipAddress;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(String subscriberId) {
        this.subscriberId = subscriberId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplicationUser)) return false;

        ApplicationUser that = (ApplicationUser) o;

        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;
        if (subscriberId != null ? !subscriberId.equals(that.subscriberId) : that.subscriberId != null) return false;
        return ipAddress != null ? ipAddress.equals(that.ipAddress) : that.ipAddress == null;

    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (subscriberId != null ? subscriberId.hashCode() : 0);
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ApplicationUser{" +
                "uuid='" + uuid + '\'' +
                ", subscriberId='" + subscriberId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                '}';
    }
}
