package io.mewbase.common;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;

/**
 * Created by tim on 22/09/16.
 */
public class SubDescriptor {

    public static final long DEFAULT_START_POS = -1;
    public static final long DEFAULT_START_TIME = 0;

    private String channel;
    private String durableID;
    private long startPos = DEFAULT_START_POS;
    private long startTimestamp = DEFAULT_START_TIME;
    private String filterName;
    private String group;

    public String getDurableID() {
        return durableID;
    }

    public SubDescriptor setDurableID(String durableID) {
        this.durableID = durableID;
        return this;
    }

    public String getChannel() {
        return channel;
    }

    public SubDescriptor setChannel(String channel) {
        this.channel = channel;
        return this;
    }

    public long getStartPos() {
        return startPos;
    }

    public SubDescriptor setStartPos(long startPos) {
        this.startPos = startPos;
        return this;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public SubDescriptor setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        return this;
    }

    public String getFilterName() {
        return filterName;
    }

    public SubDescriptor setFilterName(String filterName) {
        this.filterName = filterName;
        return this;
    }


    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubDescriptor that = (SubDescriptor) o;

        if (startTimestamp != that.startTimestamp) return false;
        if (startPos != that.startPos) return false;
        if (durableID != null ? !durableID.equals(that.durableID) : that.durableID != null) return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null) return false;
        if (filterName != null ? !filterName.equals(that.filterName) : that.filterName != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;

    }

    @Override
    public int hashCode() {
        int result = durableID != null ? durableID.hashCode() : 0;
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (int) (startPos ^ (startPos >>> 32));
        result = 31 * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (filterName != null ? filterName.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

}
