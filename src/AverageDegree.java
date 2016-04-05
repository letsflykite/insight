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
    /* ========== data structures ========== */
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
    /*
     * ---------- 1. BinaryHeap ---------- min heap of tweet timestamps only insertions and
     * deletions/extractions are needed using BinaryHeap implementation from
     * org.apache.commons.collections
     */
//    PriorityQueue<TweetComparator> dateHeap = new PriorityQueue<TweetComparator>();

    /*
     * ---------- 2. HashMap<Date, ArrayList<String>> ---------- Hashtable with tweet dates as keys
     * and list of edge Strings formed by the hashtags as values example tweet: Date=>date123,
     * Hashtags=> #a, #b, #c HashMap entry: < date123: < "a-b", "a-c", "b-c" > > using standard Java
     * HashMap implementation
     */
    HashMap<Date, ArrayList<String>> dateEdgeMap = new HashMap<>();

    /*
     * ---------- 3. HashMap<String, Integer> ---------- Hashtable with edge Strings as keys and how
     * many tweets contributed those edges as values example tweets: tweet1=> #a, #b, #c; tweet2=>
     * #a, #c respective edges: tweet1=> a-b, a-c, b-c; tweet2=> a-c HashMap entries: <"a-b": 1>,
     * <"a-c": 2>, <"b-c": 1> "a-c" has value 2 because 2 tweets contributed to that edge we
     * maintain contributions only from tweets in the 60 second window using standard Java HashMap
     * implementation
     */
    HashMap<String, Integer> edgeContributionMap = new HashMap<>();

    /*
     * ---------- 4. HashMap<String, Integer> ---------- Hashtable with vertices(hashtags) as keys
     * and degrees as values example tweets: tweet1=> #a, #b, #c; tweet2=> #a, #d HashMap entries:
     * <"a": 3>, <"b": 2>, <"c": 2>, <"d": 1> "a" has 3 edges to "b", "c" and "d"; "d" has only 1
     * edge to "a" this HashMap lets us determine when a vertex is not connected to anyone else
     */
    HashMap<String, Integer> vertexDegreeMap = new HashMap<>();

    private String currentAvgDegree;

    public Date maxTimestamp; // current max timestamp processed
    private int nodes;
    // private float vertex;
//    private PriorityQueue<Node> graph;

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

    // private void updateGraphbyTime() {
    // Date minTimestampinQ = graph.peek().get_ttl();
    //
    // if ((maxTimestamp.getTime() - minTimestampinQ.getTime()) >= 60000) {// evict those 60s or
    // // older
    //
    // }
    // }

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
     * Will check if a node is already in the graph with the same name Will not insert and stop
     * immediately if it finds one
     * 
     * @param n
     */
    // private void add_Node(Node n) {
    //
    // for (int t = 0; t < graph.size(); t++) {
    // if (graph.get(t).name.equals(n.name)) {
    // break;
    // } else if (!graph.get(t).name.equals(n.name) && t == graph.size() - 1) {
    // nodes += 1;
    // graph.add(n);
    // }
    //
    // /**
    // * Our cleanup starts here Cleanup will go through the nodes and check out the
    // * timestamps. If the timestamp is out of our 60 sec range, it will enter said node to
    // * see what edges have expired and remove them. If there are no more edges, the node is
    // * deleted as well.
    // **/
    // if (maxTimestamp.getTime() - (graph.get(t).get_ttl().getTime()) > 60000) {
    // Date lowest = maxTimestamp;
    // for (int e = 0; e < graph.get(t).edges.size(); e++) {
    // if (maxTimestamp.getTime() - graph.get(t).edges.get(e).get_time().getTime() > 60000) {
    // graph.get(t).remove_edge(graph.get(t).edges.get(e));
    // } else if (lowest.after(graph.get(t).edges.get(e).get_time())) {
    // lowest = graph.get(t).edges.get(e).get_time();
    // }
    // }
    // /*
    // * the graph should only contain connected nodes, and this also means that you may
    // * need to remove nodes if they are no longer connected once tweets are evicted from
    // * the 60-second window.
    // */
    // if (graph.get(t).get_Num_Edges() <= 0) {
    // remove_Node(graph.get(t));
    // } else {
    // graph.get(t).set_ttl(lowest);
    // }
    // }
    // }
    // }
    //
    // private void remove_Node(Node n) {
    // if (graph.contains(n) == true) {
    // nodes -= 1;
    // graph.remove(n);
    // }
    // }

    /**
     * This is our function to find our average vertex before we output it to a file
     */
    private void count() {
        // int sum = 0;
        // for (int i = 0; i < graph.size(); i++) {
        // sum += graph.get(i).get_Num_Edges();
        // }
        long numEdges = hashTagEdgeCount.size();
        long numVertices = hashTagVerticeCount.size();
        if ((numEdges> 0)&&(numVertices>0)) {
            double avgDegreeCount = 2.0 * (double) numEdges / (double) numVertices;
            setCurrentAvgDegree(String.format("%.2f", avgDegreeCount));
        }
        else
            setCurrentAvgDegree("0.00");
    }

    // private void updateAvgDegree() {
    // return String.format("%.2f", vertex);
    // }

    /*
     * private static class EventsimProcessorDef implements ProcessorDef {
     * 
     * @Override public Processor<String, String> instance() { return new Processor<String,
     * String>() { private ProcessorContext context; private KeyValueStore<String, String> kvStore;
     * ObjectMapper mapper;
     * 
     * private HashMap<String, Long> artistToPlayCount; private HashMap<String, Long>
     * songToPlayCount;
     * 
     * private MinMaxPriorityQueue<Pair<String, Long>> artistToPlayCountQueue; private
     * MinMaxPriorityQueue<Pair<String, Long>> songToPlayCountQueue;
     * 
     * private static final int TOP_N = 10;
     * 
     * private AtomicInteger currentGeneration = new AtomicInteger(0);
     * 
     * @Override public void init(ProcessorContext context) { this.context = context;
     * this.context.schedule(windowsizems); this.kvStore = new InMemoryKeyValueStore<String,
     * String>( "local-state", context); this.mapper = new ObjectMapper();
     * 
     * artistToPlayCount = new HashMap<String, Long>(); songToPlayCount = new HashMap<String,
     * Long>();
     * 
     * createQueues(); }
     * 
     * private void createQueues() { Comparator<Pair<String, Long>> comparator = new
     * Comparator<Pair<String, Long>>() {
     * 
     * @Override public int compare(Pair<String, Long> o1, Pair<String, Long> o2) { return
     * o1.getValue().compareTo(o2.getValue()) * -1; }
     * 
     * };
     * 
     * artistToPlayCountQueue = MinMaxPriorityQueue
     * .orderedBy(comparator).maximumSize(TOP_N).create(); songToPlayCountQueue =
     * MinMaxPriorityQueue .orderedBy(comparator).maximumSize(TOP_N).create(); }
     * 
     * @Override public void process(String key, String value) { String mapKey = getKeyName(value);
     * String oldValue = this.kvStore.get(mapKey);
     * 
     * if (oldValue == null) { // Swap k/v around as eventsim key is null this.kvStore.put(mapKey,
     * value); } else { // TODO: Handle when k/v already there // this.kvStore.put(key, oldValue +
     * newValue); }
     * 
     * context.commit(); }
     * 
     * @Override public void punctuate(long streamTime) { currentGeneration.incrementAndGet();
     * 
     * KeyValueIterator<String, String> iter = this.kvStore.all();
     * 
     * double totalDuration = 0;
     * 
     * long totalEntries = 0;
     * 
     * while (iter.hasNext()) { Entry<String, String> entry = iter.next();
     * 
     * totalEntries++;
     * 
     * if (entry.value() != null) { try { JsonNode rootNode = mapper.readTree(entry .value()); // /*
     * // * Example input: // * {"ts":1442428043000,"userId":23545, // * "sessionId":23544 // *
     * ,"page":"NextSong","auth":"Logged In" // * ,"method":"PUT" // *
     * ,"status":200,"level":"paid","itemInSession" // * :35,"location" // * :
     * "New York-Newark-Jersey City, NY-NJ-PA" // * ,"userAgent": // *
     * "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36\""
     * // * ,"lastName":"Barnes","firstName":"Camila", // * "registration" // *
     * :1442043066000,"gender":"F","artist" // * :"Radiohead","song" // * :"Creep (Explicit)"
     * ,"duration":235.7024} // JsonNode artist = rootNode.path("artist"); JsonNode song =
     * rootNode.path("song"); JsonNode duration = rootNode.path("duration");
     * 
     * addOrUpdate(artist.asText(), artistToPlayCount); addOrUpdate(song.asText(), songToPlayCount);
     * 
     * totalDuration += duration.asDouble();
     * 
     * if (checkDelete(entry.key())) { iter.remove(); } } catch (Exception e) { e.printStackTrace();
     * } } }
     * 
     * iter.close();
     * 
     * context.forward(null, output(totalDuration, totalEntries));
     * 
     * // Clear things for the next window artistToPlayCount.clear(); songToPlayCount.clear(); }
     * 
     * private boolean checkDelete(String key) { // Use a rolling window and generations to keep
     * more data String[] parts = key.split("-");
     * 
     * try { int gen = Integer.parseInt(parts[1]);
     * 
     * if (gen < currentGeneration.get() - maximumGenerations) { return true; } } catch (Exception
     * e) { logger.debug(e); }
     * 
     * return false; }
     * 
     * private String output(double totalDuration, long totalEntries) { StringBuilder builder = new
     * StringBuilder("{");
     * 
     * // The total amount played builder.append("\"totalduration\":").append(totalDuration)
     * .append(",");
     * 
     * // Add the artist plays builder.append("\"artisttoplaycount\": ["); processCounts(builder,
     * artistToPlayCount, artistToPlayCountQueue); builder.append("], ");
     * 
     * // Add the song plays builder.append("\"songtoplaycount\": ["); processCounts(builder,
     * songToPlayCount, songToPlayCountQueue); builder.append("],");
     * 
     * // Add the other totals builder.append("\"totals\": {"); builder.append("\"totalentries\": "
     * ).append(totalEntries) .append(","); builder.append("\"totalsongs\": ")
     * .append(songToPlayCount.size()).append(","); builder.append("\"totalartists\": ").append(
     * artistToPlayCount.size()); builder.append("} }");
     * 
     * return builder.toString(); }
     * 
     * private void processCounts(StringBuilder builder, HashMap<String, Long> map,
     * MinMaxPriorityQueue<Pair<String, Long>> queue) { Iterator<java.util.Map.Entry<String, Long>>
     * iterator = map .entrySet().iterator();
     * 
     * queue.clear();
     * 
     * while (iterator.hasNext()) { java.util.Map.Entry<String, Long> entry = iterator .next();
     * queue.add(new Pair<String, Long>(entry.getKey(), entry .getValue())); }
     * 
     * Iterator<Pair<String, Long>> queueIter = queue.iterator();
     * 
     * while (queueIter.hasNext()) { Pair<String, Long> entry = queueIter.next();
     * 
     * builder.append("{\"name\" : \"").append(entry.getKey()) .append("\",");
     * 
     * builder.append("\"count\" : ").append(entry.getValue()) .append("}");
     * 
     * if (queueIter.hasNext()) { builder.append(","); } } }
     * 
     * private void addOrUpdate(String key, HashMap<String, Long> map) { // Make sure the key isn't
     * an empty string if (key.equals("")) { return; } else { Long currentValue = map.get(key); if
     * (currentValue == null) { map.put(key, 1L); } else { map.put(key, currentValue + 1L); } } }
     * 
     * @Override public void close() { this.kvStore.close(); }
     * 
     * private String getKeyName(String value) { return String.valueOf(value.hashCode()) + "-" +
     * currentGeneration.get(); } }; } }
     * 
     */

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

            // insert in graph
            // add_Node(new Node(hashTag, timeOfCurrentTweet));
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
        // Start the File read in process

//        String line = new String();
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

                    // // Create Nodes and edges based on the hashtags.
                    // // If they already exist update the timestamps
                    // // This step will be skipped if the tweet only
                    // // Has one tag or less
                    // if (hashtagsListinCurrentTweet.size() > 1) {
                    // for (int i = 0; i < hashtagsListinCurrentTweet.size(); i++) {
                    // // if the hashtag exists as a vertex?
                    //
                    // avgDegree.add_Node(new Node(hashtagsListinCurrentTweet.get(i),
                    // timeOfCurrentTweet));
                    // for (int x = 0; x < hashtagsListinCurrentTweet.size(); x++) {
                    // if (x != i) {
                    // avgDegree.graph.get(i).add_edge(new Edge(hashtagsListinCurrentTweet.get(x),
                    // timeOfCurrentTweet));
                    // }
                    // }
                    // }
                    // }
                    // avgDegree.updateGraphbyTime();
                    /**
                     * Our cleanup starts here Cleanup will go through the nodes and check out the
                     * timestamps. If the timestamp is out of our 60 sec range, it will enter said
                     * node to see what edges have expired and remove them. If there are no more
                     * edges, the node is deleted as well.
                     **/
                    // for (int t = 0; t < avgDegree.graph.size(); t++) {
                    // if (avgDegree.maxTimestamp.getTime() -
                    // (avgDegree.graph.get(t).get_ttl().getTime()) > 60000) {
                    // Date lowest = avgDegree.maxTimestamp;
                    // for (int e = 0; e < avgDegree.graph.get(t).edges.size(); e++) {
                    // if (avgDegree.maxTimestamp.getTime() -
                    // avgDegree.graph.get(t).edges.get(e).get_time().getTime() > 60000) {
                    // avgDegree.graph.get(t).remove_edge(avgDegree.graph.get(t).edges.get(e));
                    // } else if (lowest.after(avgDegree.graph.get(t).edges.get(e).get_time())) {
                    // lowest = avgDegree.graph.get(t).edges.get(e).get_time();
                    // }
                    // }
                    // /*
                    // * the graph should only contain connected nodes, and this also means
                    // * that you may need to remove nodes if they are no longer connected
                    // * once tweets are evicted from the 60-second window.
                    // */
                    // if (avgDegree.graph.get(t).get_Num_Edges() <= 0) {
                    // avgDegree.remove_Node(avgDegree.graph.get(t));
                    // } else {
                    // avgDegree.graph.get(t).set_ttl(lowest);
                    // }
                    // }
                    // }
                    // //recalculate
                    // avgDegree.count();

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
