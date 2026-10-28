package am.ik.sluice.it;

import java.util.concurrent.CopyOnWriteArrayList;

import ch.qos.logback.core.read.ListAppender;

/**
 * A {@link ListAppender} whose {@code list} can be read while server threads are still
 * appending to it.
 */
final class ConcurrentListAppender<E> extends ListAppender<E> {

	ConcurrentListAppender() {
		this.list = new CopyOnWriteArrayList<>();
	}

}
