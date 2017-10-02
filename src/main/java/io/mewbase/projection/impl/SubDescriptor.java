package io.mewbase.projection.impl;

/**
 * Created by tim on 22/09/16.
 */
public class SubDescriptor {

    public static final long DEFAULT_START_NUM = -1;
    public static final long DEFAULT_START_TIME = 0;

    private String channel;
    private String durableID;
    private long startEventNum = DEFAULT_START_NUM;
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

    public long getStartEventNum() {
        return startEventNum;
    }

    public SubDescriptor setStartEventNum(long startEventNum) {
        this.startEventNum = startEventNum;
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
        if (startEventNum != that.startEventNum) return false;
        if (durableID != null ? !durableID.equals(that.durableID) : that.durableID != null) return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null) return false;
        if (filterName != null ? !filterName.equals(that.filterName) : that.filterName != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;

    }

    @Override
    public int hashCode() {
        int result = durableID != null ? durableID.hashCode() : 0;
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (int) (startEventNum ^ (startEventNum >>> 32));
        result = 31 * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (filterName != null ? filterName.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

}
