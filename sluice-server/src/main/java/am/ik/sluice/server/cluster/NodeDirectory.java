package am.ik.sluice.server.cluster;

import java.util.List;

/**
 * Cluster membership source. Implementations answer "which nodes exist right now"; the
 * server publishes the answer to connected clients, which keep one tunnel stream per
 * node.
 * <p>
 * The membership version changes whenever the returned list changes; it lets
 * {@link MembershipBroadcaster} push updates only when something actually changed.
 */
public interface NodeDirectory {

	/**
	 * One cluster member.
	 *
	 * @param nodeId stable unique id of the node
	 * @param publicUrl control plane address clients connect to for this node; empty =
	 * clients keep using their bootstrap address for it
	 */
	record NodeMember(String nodeId, String publicUrl) {

		public NodeMember {
			nodeId = nodeId == null ? "" : nodeId;
			publicUrl = publicUrl == null ? "" : publicUrl;
		}

	}

	/**
	 * Monotonically increasing membership version; a value greater than the previous one
	 * means the node list changed.
	 */
	long version();

	/**
	 * Current cluster members, including this node.
	 */
	List<NodeMember> nodes();

}
