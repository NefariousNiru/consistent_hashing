package bootstrapUtil;

import common.NodeInfo;

import java.util.TreeSet;
import java.util.Comparator;

public class NodeManager {
    private final int MAXRANGE = 1023;
    private TreeSet<NodeInfo> nodes;

    public NodeManager(int bootstrapNodePort) {
        nodes = new TreeSet<>(Comparator.comparingInt(NodeInfo::getId));
        nodes.add(new NodeInfo(0, "N/A", bootstrapNodePort));
    }

    public synchronized Response addNode(NodeInfo node) {
        int nodeId = node.getId();
        if (nodeId < 0 || nodeId > MAXRANGE) {
            return new Response(-1, "Invalid Node ID. Must be between 0 and " + MAXRANGE);
        }

        if (nodes.contains(node)) {
            return new Response(-1, "Node " + nodeId + " already exists.");
        }

        nodes.add(node);
        updatePointers();
        return new Response(0, "Node " + nodeId + " added.");
    }

    public synchronized Response removeNode(NodeInfo node) {
        int nodeId = node.getId();
        if (nodeId < 0 || nodeId > MAXRANGE) {
            return new Response(-1, "Invalid node ID. Must be between 0 and " + MAXRANGE);
        }

        if (nodeId == 0) {
            return new Response(-1, "Bootstrap node cannot be removed.");
        }

        if (nodes.contains(node)) {
            nodes.remove(node);
            updatePointers();
            return new Response(0, "Node " + nodeId + " removed.");
        }

        return new Response(-1, "Node " + nodeId + " does not exist.");
    }

    // Update each node's predecessor and successor pointers.
    private void updatePointers() {
        if (nodes.isEmpty()) return;
        NodeInfo first = nodes.first();
        NodeInfo last = nodes.last();
        for (NodeInfo node : nodes) {
            NodeInfo pred = nodes.lower(node);  // The predecessor is the node immediately lower in ID, or the last if none exists.
            if (pred == null) {
                pred = last;
            }

            NodeInfo succ = nodes.higher(node); // The successor is the node immediately higher in ID, or the first if none exists.
            if (succ == null) {
                succ = first;
            }
            node.setPredecessor(pred);
            node.setSuccessor(succ);
        }
    }

    public NodeInfo getNodeById(int nodeId) {
        for (NodeInfo node : nodes) {
            if (node.getId() == nodeId) {
                return node;
            }
        }
        return null;
    }

    public synchronized void printNodes() {
        System.out.println("Current Node Pointers:");
        for (NodeInfo node : nodes) {
            System.out.println(node);
        }
    }
}
