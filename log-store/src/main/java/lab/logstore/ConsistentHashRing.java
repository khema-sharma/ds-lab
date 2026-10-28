package lab.logstore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

public final class ConsistentHashRing {

    private final int virtualNodesPerNode;
    private final TreeMap<String, String> ring;

    public ConsistentHashRing(int numberOfVirtualNodes) {
        if (numberOfVirtualNodes == 0) {
            throw new IllegalArgumentException("numberOfVirtualNodes must be atleast 1.");
        }

        virtualNodesPerNode = numberOfVirtualNodes;
        ring = new TreeMap<>();
    }

    public void addNode(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId cannot be blank.");
        }

        for (int i = 0; i < virtualNodesPerNode; i++) {
            ring.put(hash(nodeId + '#' + i), nodeId);
        }
    }

    public String nodeForKey(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("ring is empty.");
        }

        String keyHash = hash(key);

        Map.Entry<String, String> entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }

        return entry.getValue();

    }

    private String hash(String value) {
        MessageDigest mDigest = null;
        try {
            mDigest = MessageDigest.getInstance("MD5");
            byte[] hashBytes = mDigest.digest(value.getBytes(StandardCharsets.UTF_8));

            String md5Hash = HexFormat.of().formatHex(hashBytes);

            return md5Hash;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return value;
    }

}
