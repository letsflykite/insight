//package tweet.hashtag;

import java.util.Comparator;
public class TweetComparator implements Comparator<Tweet> {

    @Override
    public int compare(Tweet x, Tweet y)
    {
        if (x==null) return -1;
        if (y==null) return 1;
        return (x.compareTo(y));
    }
}

