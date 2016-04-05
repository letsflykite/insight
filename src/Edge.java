//package tweet.hashtag;
//
////import java.util.Date;

public class Edge {
    public String node1;
    public String node2;
    
    public Edge(String node1, String node2) {
        this.node1 = node1;
        this.node2 = node2;
    }

    public String getNode1() {
        return node1;
    }

    public void setNode1(String node1) {
        this.node1 = node1;
    }

    public String getNode2() {
        return node2;
    }

    public void setNode2(String node2) {
        this.node2 = node2;
    }

    @Override
    public int hashCode() {
        return (hashCode1()+hashCode2());
    }
    
    private int hashCode1() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((node1 == null) ? 0 : node1.hashCode());
        result = prime * result + ((node2 == null) ? 0 : node2.hashCode());
        return result;
    }
    private int hashCode2() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((node2 == null) ? 0 : node2.hashCode());
        result = prime * result + ((node1 == null) ? 0 : node1.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        return equals1(obj)||equals2(obj);
    }
    
  //node1 == other.node1, node2 == other.node2
    private boolean equals1(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Edge other = (Edge) obj;
        if (node1 == null) {
            if (other.node1 != null)
                return false;
        } else if (!node1.equals(other.node1))
            return false;
        if (node2 == null) {
            if (other.node2 != null)
                return false;
        } else if (!node2.equals(other.node2))
            return false;
        return true;
    }
    
    //node1 == other.node2, node2 == other.node1
    private boolean equals2(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Edge other = (Edge) obj;
        if (node1 == null) {
            if (other.node2 != null)
                return false;
        } else if (!node1.equals(other.node2))
            return false;
        if (node2 == null) {
            if (other.node1 != null)
                return false;
        } else if (!node2.equals(other.node1))
            return false;
        return true;
    }

}   
