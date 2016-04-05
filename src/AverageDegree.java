//package tweet.hashtag;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
//import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class AverageDegree {

    // hashtag list, each entry is like <[Apache, Hadoop, Storm], timestamp>
    private PriorityQueue<Tweet> tweets = new PriorityQueue<Tweet>(new TweetComparator());
    
    // vertex list: <vertice, count>： <Apache, 1>, <Hadoop, 1>, <Storm, 1>
    // private HashMap<String, Long> hashTagVerticeCount;
    private HashMap<String, TagInfo> hashTagVerticeCount;
    // edge list: <edge, count>: <Apache, Hadoop, 1>, <Apache, Storm, 1>, <Hadoop, Storm, 1>
    private HashMap<Edge, Long> hashTagEdgeCount;

    /*
     * 
     * 
     * 
     * for each vertex mentioned in the hashtag: increment count for the vertex in the vertex list.
     * for each edge mentioned in the hashtag: increment count for the edge in the edge list.
     * 
     * while the oldest hashtag is older than 60 seconds: old_hashtag = the oldest hashtag; remove
     * old_hashtag from the hashtag list;
     * 
     * for each vertex mentioned in the hashtag: decrement count for the vertex in the vertex list;
     * if count == 0: remove vertex from the vertex list;
     * 
     * for each edge mentioned in the hashtag: decrement count for the edge in the edge list; if
     * count == 0: remove edge from the edge list;
     * 
     * E = number of edges in the edge list; V = number of vertices in the vertex list;
     * average_degree = (2 * E) / V; output average_degree;
     */

    private String currentAvgDegree;

    public Date maxTimestamp; // current max timestamp processed
    private int nodes;

    public AverageDegree() {
        // public List<Node> graph;
        tweets = new PriorityQueue<Tweet>();
        hashTagVerticeCount = new HashMap<String, TagInfo>();
        hashTagEdgeCount = new HashMap<Edge, Long>();
        currentAvgDegree = "0.00";
        maxTimestamp = null;
    }

    public String getCurrentAvgDegree() {
        return currentAvgDegree;
    }

    public void setCurrentAvgDegree(String currentAvgDegree) {
        this.currentAvgDegree = currentAvgDegree;
    }
    /*
     * the graph should only contain connected nodes, and this also means that you may need to
     * remove nodes if they are no longer connected once tweets are evicted from the 60-second
     * window.
     */

    //    final static Logger logger = Logger.getLogger(AverageDegree.class);



    // evict old tweets, remove edges, remove vertices
    public void evictTweets() {
        if (tweets==null || tweets.isEmpty()) return;
        // while oldestTweet is equal or older Than 60s, remove it from min heap
        // the 60s window is exclusive
        Date minTimestampinTweets = tweets.peek().getTimeCreated();

        while ((maxTimestamp.getTime() - minTimestampinTweets.getTime()) >= 60000) {
            // out of the 60s window
            Tweet tweetEvicted = tweets.poll();

            List<String> hashTagListInEvictedTweet = tweetEvicted.getHashTagNames();
            int nLenHashTagListInEvictedTweet = hashTagListInEvictedTweet.size();
            for (int i = 0; i < nLenHashTagListInEvictedTweet; i++) {
                String hashTag = hashTagListInEvictedTweet.get(i);

                // form edges to be removed
                for (int j = i + 1; j < nLenHashTagListInEvictedTweet; j++) {
                    String hashTag2 = hashTagListInEvictedTweet.get(j);
                    Edge edge = new Edge(hashTag, hashTag2);
                    // when an edge is removed, update numOfEdges for both nodes' hashTag. this num
                    // of edges counts for redundancy
                    if (hashTagVerticeCount.get(hashTag)!=null) {
                        long numOfNeighborsHashTag1 = hashTagVerticeCount.get(hashTag).getNumOfNeighbors() - 1L;
                        // if no connection, remove the node
                        if (numOfNeighborsHashTag1 == 0)
                            hashTagVerticeCount.remove(hashTag);
                        else
                            hashTagVerticeCount.get(hashTag).setNumOfNeighbors(numOfNeighborsHashTag1);
                    }
                    if (hashTagVerticeCount.get(hashTag2)!=null) {
                        long numOfNeighborsHashTag2 = hashTagVerticeCount.get(hashTag2).getNumOfNeighbors() - 1L;
                        if (numOfNeighborsHashTag2 == 0)
                            hashTagVerticeCount.remove(hashTag2);
                        else
                            hashTagVerticeCount.get(hashTag2).setNumOfNeighbors(numOfNeighborsHashTag2);
                    }
                    
                    // update edge counter
                    // is this edge existing?
                    if (!hashTagEdgeCount.containsKey(edge)) {// edge does not exist or already
                                                              // removed
                        continue; // check the next edge
                    } else {// not removed yet
                        long updatedEdgeCount = hashTagEdgeCount.get(edge) - 1L;
                        if (updatedEdgeCount == 0)
                            hashTagEdgeCount.remove(edge);
                        else
                            hashTagEdgeCount.put(edge, hashTagEdgeCount.get(edge) - 1L);
                    }
                    
                    //remove vertices

                    removeVertice(hashTag2);
                }
                removeVertice(hashTag);

            }
            if (tweets.isEmpty()) return;
            // check the next oldest tweet
            minTimestampinTweets = tweets.peek().getTimeCreated();
        }
    }

    private void removeVertice(String hashTag) {
        // update hashtag vertex counter
        TagInfo tagInfo = hashTagVerticeCount.get(hashTag);
        if (tagInfo == null) // does not exist or already removed
            return; // check next hashtag in list
        long updatedTagCount = hashTagVerticeCount.get(hashTag).getNumOfTags() - 1;
        long updatedConnectionCount = hashTagVerticeCount.get(hashTag).getNumOfNeighbors() - 1;
        if ((updatedTagCount == 0) || (updatedConnectionCount == 0)) {// remove from hashmap
            hashTagVerticeCount.remove(hashTag);
        } else {
            hashTagVerticeCount.put(hashTag, new TagInfo(updatedTagCount, updatedConnectionCount));
        }
    }

    /**
     * This is our function to find our average vertex before we output it to a file
     */
    private void count() {
    
        long numEdges = hashTagEdgeCount.size();
        long numVertices = hashTagVerticeCount.size();
        if ((numEdges> 0)&&(numVertices>0)) {
            double avgDegreeCount = 2.0 * (double) numEdges / (double) numVertices;
            setCurrentAvgDegree(String.format("%.2f", avgDegreeCount));
        }
        else
            setCurrentAvgDegree("0.00");
    }

    
    
    // if the tweet has valid hashtags, insert hashtag list in tweets queue, add vertices, add edges
    public void addTweet(Date timeOfCurrentTweet, String created_at, List<String> hashtagsListinCurrentTweet) {
        int nLenTagList = hashtagsListinCurrentTweet.size();
        // if the tweet has valid hashtags, insert hashtag list in tweets queue
        if (nLenTagList > 0) {
            Tweet tweet = new Tweet(created_at, timeOfCurrentTweet, hashtagsListinCurrentTweet);
            tweets.add(tweet);
        }
        // update vertice, edge counter
        for (int i = 0; i < nLenTagList; i++) {
            String hashTag = hashtagsListinCurrentTweet.get(i);
            insertVertice(hashTag);

            // form edges
            for (int j = i + 1; j < nLenTagList; j++) {
                String hashTag2 = hashtagsListinCurrentTweet.get(j);
                // insert vertex first
                insertVertice(hashTag2);
                Edge edge = new Edge(hashTag, hashTag2);
                // when a new edge is formed, update numOfEdges for both nodes' hashTag. this num of
                // edges counts for redundancy
                long numOfNeighborsHashTag1 = hashTagVerticeCount.get(hashTag).getNumOfNeighbors() + 1L;
                hashTagVerticeCount.get(hashTag).setNumOfNeighbors(numOfNeighborsHashTag1);
                long numOfNeighborsHashTag2 = hashTagVerticeCount.get(hashTag2).getNumOfNeighbors() + 1L;
                hashTagVerticeCount.get(hashTag2).setNumOfNeighbors(numOfNeighborsHashTag2);

                // update edge counter
                // is this a new edge?
                if (hashTagEdgeCount!=null&&!hashTagEdgeCount.containsKey(edge)) {// edge does not exist
                    hashTagEdgeCount.put(edge, 1L);
                } else {// not new edge
                    hashTagEdgeCount.put(edge, 1L + hashTagEdgeCount.get(edge));
                }
            }
        }
    }

    private void insertVertice(String hashTag) {
        // update vertice counter. this num of tags counts for redundancy
        if (hashTagVerticeCount.get(hashTag) == null) {// hashtag does not exist in graph
            // new vertex, it does not have any connection, set the num of neighbors to zero
            hashTagVerticeCount.put(hashTag, new TagInfo(1L, 0));


        } else {// hashtag exists in graph, increase counter
            // not new vertex, increase the num of tags counts, leave the num of neighbors unchanged
            // any new edge
            hashTagVerticeCount.put(hashTag, new TagInfo(1L + hashTagVerticeCount.get(hashTag).getNumOfTags()));
        }
    }

    public void write(FileWriter fw) {
        try {
            // avgDegree.setCurrentAvgDegree(currentAvgDegree);
            fw.write(currentAvgDegree+"\n");
        } catch (Exception e) {
//            logger.info(e);
        }
    }

    public static void main(String[] args) throws Exception {
        // if (args.length != 2) {
        // throw new RuntimeException(args.length + " arguments supplied. Should be 1. Usage: java
        // AverageDegree inputFilePath outputFilePath"
        // + "\nExample usage: java AverageDegree ./tweet_input/tweets.txt
        // ./tweet_output/output.txt");
        // }
        String inputFileName = "./tweet_input/tweets.txt";
        String outputFileName = "./tweet_output/output.txt";

        if (args.length == 2) {
            inputFileName = args[0];
            outputFileName = args[1];
        }
        /*
         * The output should be a file in the tweet_output directory named output.txt that contains
         * the rolling average for each tweet in the file (e.g. if there are three input tweets,
         * then there should be 3 averages), following the format above. The precision of the
         * average should be two digits after the decimal place with truncation.
         */

        ObjectMapper mapper = new ObjectMapper();;
        AverageDegree avgDegree = new AverageDegree();
        String strLine;
        Date timeOfCurrentTweet;
        final String df = "EEE MMM dd HH:mm:ss Z yyyy"; // "Mon Mar 28 23:23:12 +0000 2016"

        try {
            // read file line by line
            BufferedReader br = new BufferedReader(new FileReader(inputFileName));
            // Write our average vertex to the out put file
            System.out.println("outputfileName: "+outputFileName);
            FileWriter fw = new FileWriter(outputFileName);
            long nlineNum = 0;
            int nIgnore=0;
            while ((strLine = br.readLine()) != null) {
                try {
                    nlineNum ++;

                    JsonNode json = mapper.readTree(strLine);
                    JsonNode created = json.path("created_at");
                    if (created.isMissingNode()) {
                        nIgnore++;
//                        logger.info(nIgnore+"th skipped line "+nlineNum+": "+strLine);
                        continue; // ignore the rate limit
                    }
                    SimpleDateFormat sf = new SimpleDateFormat(df, Locale.ENGLISH);
                    sf.setLenient(true);
                    String created_at = json.get("created_at").asText();
                    timeOfCurrentTweet = sf.parse(created_at);

                    if (avgDegree.maxTimestamp == null) // first tweet
                        avgDegree.maxTimestamp = timeOfCurrentTweet;
                    // check if it is out of order?
                    // if the tweet is outside the window (60 s older), graph does not change, need
                    // to write out current average degree.
                    if ((avgDegree.maxTimestamp.getTime() - timeOfCurrentTweet.getTime()) >= 60000) {
//                        logger.info("avgDeree for line "+nlineNum+": "+avgDegree.getCurrentAvgDegree());
                        System.out.println("avgDeree for line "+nlineNum+": "+avgDegree.getCurrentAvgDegree());
                        avgDegree.write(fw);
                        continue; // move to the next tweet
                    } else if (avgDegree.maxTimestamp.before(timeOfCurrentTweet)) { // <0
                        // if the tweet is younger, even if no new hashtags, need to recalcuate the
                        // edges based on the new max.
                        avgDegree.maxTimestamp = timeOfCurrentTweet;// update the max timestamp if
                                                                    // this tweet is newer
                        // update the tweets
                        // recalculate the edges
                        // avgDegree.count();//?
                        // write out
                        // write(avgDegree, fw);
                        // continue; // move to the next tweet
                    } // else { // if the tweet is within 60s older, need to recalculate average
                      // degree. [0, 6000)

                    // }

                    // the new tweet is either younger or less than 60s older

                    // if there are new hashtags, add new nodes and edges if any

                    // extract the hashtags from the tweet and insert them to the hashtag list;
                    List<String> hashtagsListinCurrentTweet = new ArrayList<String>();
                    JsonNode entities = json.path("entities");
                    if (!entities.isMissingNode()) {
                        JsonNode arrNode = entities.path("hashtags");
                        if (!arrNode.isMissingNode() && arrNode.isArray()) {
                            /*
                             * if the number of hashtags are less than 2, no new node/edge can be
                             * inserted, only recalculate the graph based on timestamp
                             */
                            if (arrNode.size() >= 2) {
                                for (final JsonNode objNode : arrNode) {
                                    // System.out.println(objNode);
                                    // hashtagsListinCurrentTweet.add(objNode.get("text").asText());
                                    // a list of hashtags
                                    String hashTag = objNode.get("text").asText();
                                    // insert in hashtaglist
                                    if (hashTag != null)
                                        hashtagsListinCurrentTweet.add(hashTag);
                                }
                            }
                        }
                    }
                    //logger.info("line "+nlineNum +" "+hashtagsListinCurrentTweet);
                    int nLenTagList = hashtagsListinCurrentTweet.size();
                    // if the tweet has valid hashtags, insert hashtag list in tweets queue, updated
                    // vertice, edge counters
                    if (nLenTagList > 0) {
                        avgDegree.addTweet(timeOfCurrentTweet, created_at, hashtagsListinCurrentTweet);
                    }
                    // evict old tweets
                    // while oldestTweet is Older Than 60s, remove it from min heap
                    // update vertice, edge counters
                    avgDegree.evictTweets();

                    // recalculate
                    avgDegree.count();


//                    logger.info("avgDeree for line "+nlineNum+": "+avgDegree.getCurrentAvgDegree());
                    System.out.println("avgDeree for line "+nlineNum+": "+avgDegree.getCurrentAvgDegree());
                    avgDegree.write(fw);
                } catch (Exception e) {
//                    logger.info(e);
                }
            }
            fw.close();
        } catch (Exception e) {
//            logger.info(e);
        }

    }

}
