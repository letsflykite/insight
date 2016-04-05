//package tweet.hashtag;


public class TagInfo {
    private long numOfTags;
    private long numOfNeighbors; // how many edges this tag vertex belong to
    public TagInfo(long numOfTags) {
        this.numOfTags = numOfTags;
    }
    public TagInfo(long numOfTags, long numOfNeighbors) {
        this.numOfTags = numOfTags;
        this.numOfNeighbors = numOfNeighbors;
    }
    public long getNumOfTags() {
        return numOfTags;
    }
    public void setNumOfTags(long numOfTags) {
        this.numOfTags = numOfTags;
    }
    public long getNumOfNeighbors() {
        return numOfNeighbors;
    }
    public void setNumOfNeighbors(long numOfNeighbors) {
        this.numOfNeighbors = numOfNeighbors;
    }
}
