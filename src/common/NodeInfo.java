package common;

public class NodeInfo {
    private int id;
    private String ip;
    private int port;
    private NodeInfo predecessor;
    private NodeInfo successor;

    public NodeInfo(int id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public NodeInfo getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(NodeInfo predecessor) {
        this.predecessor = predecessor;
    }

    public NodeInfo getSuccessor() {
        return successor;
    }

    public void setSuccessor(NodeInfo successor) {
        this.successor = successor;
    }

    @Override
    public String toString() {
        String pred = (predecessor == null) ? "null" : predecessor.getId() + " (" + predecessor.getIp() + ":" + predecessor.getPort() + ")";
        String succ = (successor == null) ? "null" : successor.getId() + " (" + successor.getIp() + ":" + successor.getPort() + ")";
        return "Node " + id + " [" + ip + ":" + port + ", Pred: " + pred + ", Succ: " + succ + "]";
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }
}