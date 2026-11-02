/*
 * Preloaded into Chromium by the sluice-it browser tests (Chromium.java): without a
 * NETLINK_ROUTE socket it takes the network as online and watches no interface.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <linux/netlink.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

int socket(int domain, int type, int protocol) {
	if (domain == AF_NETLINK && protocol == NETLINK_ROUTE) {
		errno = EACCES;
		return -1;
	}
	return (int) syscall(SYS_socket, domain, type, protocol);
}
