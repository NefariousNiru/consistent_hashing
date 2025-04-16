package nameservers.namserverUtil;

import common.NodeInfo;

public class ResponseParser {

    // Static method to parse the bootstrap response and return predecessor and successor.
    // The expected format is:
    // "ENTER OK Predecessor: <predStr> Successor: <succStr>"
    public static NodeInfo[] parseBootstrapResponse(String response) {
        int predIndex = response.indexOf("Predecessor:");
        int succIndex = response.indexOf("Successor:");
        if (predIndex == -1 || succIndex == -1) {
            throw new IllegalArgumentException("Response format invalid");
        }
        // Extract the substring for the predecessor and successor parts.
        String predPart = response.substring(predIndex + "Predecessor:".length(), succIndex).trim();
        String succPart = response.substring(succIndex + "Successor:".length()).trim();

        NodeInfo pred = parseNodeInfo(predPart);
        NodeInfo succ = parseNodeInfo(succPart);
        return new NodeInfo[] { pred, succ };
    }

    // Helper method to parse a single node's info from a string.
    // Expected format: "Node <id> [<ip>:<port>, ...]" (we care only about the first element inside brackets)
    private static NodeInfo parseNodeInfo(String info) {
        int nodeIndex = info.indexOf("Node ");
        int bracketStart = info.indexOf("[", nodeIndex);
        int bracketEnd = info.indexOf("]", bracketStart);
        if (nodeIndex == -1 || bracketStart == -1 || bracketEnd == -1) {
            throw new IllegalArgumentException("Invalid node info format");
        }
        String idStr = info.substring(nodeIndex + "Node ".length(), bracketStart).trim();
        int nodeId = Integer.parseInt(idStr);

        String bracketContent = info.substring(bracketStart + 1, bracketEnd).trim();
        // Expect first part to be "<ip>:<port>"
        String[] parts = bracketContent.split(",");
        String ipPort = parts[0].trim();
        String[] ipPortParts = ipPort.split(":");
        if (ipPortParts.length < 2) {
            throw new IllegalArgumentException("Invalid IP:port format");
        }
        String ip = ipPortParts[0];
        int port = Integer.parseInt(ipPortParts[1]);
        return new NodeInfo(nodeId, ip, port);
    }
}
