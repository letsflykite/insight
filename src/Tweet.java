//package tweet.hashtag;

import java.util.Date;
import java.util.List;

public class Tweet implements Comparable<Tweet>{
    private String created_at;      // timestamp string
    private Date timeCreated;
    private long timestamp;         // timestamp of the tweet
    private List<String> hashTagNames; // list of hashtags in the tweet
      
    public Tweet(String created_at, Date timeCreated, List<String> hashTagNames) {
        this.created_at = created_at;
        this.timeCreated = timeCreated;
        this.hashTagNames = hashTagNames;
    }
    public String getCreated_at() {
        return created_at;
    }
    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }
    public Date getTimeCreated() {
        return timeCreated;
    }
    public void setTimeCreated(Date timeCreated) {
        this.timeCreated = timeCreated;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public List<String> getHashTagNames() {
        return hashTagNames;
    }
    public void setHashTagNames(List<String> hashTagNames) {
        this.hashTagNames = hashTagNames;
    }
    
    public String toString() {
        StringBuilder result = new StringBuilder("hashtags = [");
        for (String s : hashTagNames) {
            result.append(s).append(",");
        }
        result.insert(result.length()-1, "]");
        result.append(" created_at:");
        result.append(created_at);
        return result.toString();
    }
    
    @Override
    public int compareTo(Tweet o) {
        if (this.timeCreated.after(o.timeCreated))
            return 1;
        else if (this.timeCreated.before(o.timeCreated))
            return -1;
        return 0;
    }
}