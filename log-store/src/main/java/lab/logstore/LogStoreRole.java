package lab.logstore;

/**
 * 
 * Role taken up by the {@link AppendOnlyLogStore} in a replication setup.
 */
public enum LogStoreRole {
    /**
     * Store is the leader.
     * <p>
     * A leader does the following:
     * <ul>
     *  <li>Read and write data records ({@code LogStoreRecord})
     *  <li>Replicates every write to its followers
     *  <li>Publishes CDC events
     * </ul>
     */
    LEADER,
    /**
     * Store is a follower.
     * <p>
     * A follower does the following:
     * <ul>
     *  <li>Read data records ({@code LogStoreRecord})
     *  <li>Forward writes to the elected leader
     */
    FOLLOWER
}
