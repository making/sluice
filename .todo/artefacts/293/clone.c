// What a helper's clone costs with and without CLONE_INTO_CGROUP: a parent asks a helper (its
// child) for six children over a socket pair, each cloned with CLONE_PARENT (the asker's child,
// which reaps it by the pidfd it is sent) and exec'ing a small program; the round trip of each.
//   gcc -O2 -o clone clone.c
//   ./clone - plain                                                  # clone, no cgroup
// and, clone3 into a cgroup (vm-0 of a delegated scope):
//   systemd-run --user --scope -q -p Delegate=yes -- bash -c 'exec ./clone /sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup) cgroup'
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <linux/sched.h>

static long long now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

static int send_fd(int sock, int value, int fd) {
    struct msghdr msg = {0};
    struct iovec iov = { &value, sizeof value };
    char control[CMSG_SPACE(sizeof(int))];
    msg.msg_iov = &iov; msg.msg_iovlen = 1;
    if (fd >= 0) {
        msg.msg_control = control; msg.msg_controllen = sizeof control;
        struct cmsghdr *c = CMSG_FIRSTHDR(&msg);
        c->cmsg_level = SOL_SOCKET; c->cmsg_type = SCM_RIGHTS; c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &fd, sizeof(int));
    }
    return sendmsg(sock, &msg, 0);
}

static int recv_fd(int sock, int *value, int *fd) {
    struct msghdr msg = {0};
    struct iovec iov = { value, sizeof *value };
    char control[CMSG_SPACE(sizeof(int))];
    msg.msg_iov = &iov; msg.msg_iovlen = 1;
    msg.msg_control = control; msg.msg_controllen = sizeof control;
    int n = recvmsg(sock, &msg, MSG_CMSG_CLOEXEC);
    *fd = -1;
    struct cmsghdr *c = CMSG_FIRSTHDR(&msg);
    if (c && c->cmsg_type == SCM_RIGHTS) memcpy(fd, CMSG_DATA(c), sizeof(int));
    return n;
}

int main(int argc, char **argv) {
    // argv[1]: own cgroup dir (delegated); mode in argv[2]: "cgroup" or "plain"
    const char *dir = argv[1];
    int use_cgroup = strcmp(argv[2], "cgroup") == 0;
    char path[512];
    if (use_cgroup) {
        snprintf(path, sizeof path, "%s/wicket", dir); mkdir(path, 0755);
        snprintf(path, sizeof path, "%s/wicket/cgroup.procs", dir);
        int f = open(path, O_WRONLY); if (write(f, "0", 1) != 1) { perror("move"); return 1; } close(f);
        snprintf(path, sizeof path, "%s/cgroup.subtree_control", dir);
        f = open(path, O_WRONLY); if (write(f, "+memory +pids +cpu", 18) != 18) { perror("enable"); return 1; } close(f);
        snprintf(path, sizeof path, "%s/vm-0", dir); mkdir(path, 0755);
    }
    int pair[2];
    socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, pair);
    pid_t wicket = getpid();
    pid_t helper = fork();
    if (helper == 0) {
        close(pair[0]);
        prctl(PR_SET_PDEATHSIG, SIGKILL);
        prctl(PR_SET_DUMPABLE, 0);
        for (;;) {
            int cgroup, req;
            int n = recv_fd(pair[1], &req, &cgroup);
            if (n <= 0) _exit(0);
            int pidfd = -1;
            long pid;
            if (cgroup >= 0) {
                struct clone_args args = {0};
                args.flags = CLONE_PARENT | CLONE_PIDFD | CLONE_INTO_CGROUP;
                args.pidfd = (unsigned long long)&pidfd;
                args.exit_signal = 0;
                args.cgroup = cgroup;
                pid = syscall(SYS_clone3, &args, sizeof args);
            } else {
                pid = syscall(SYS_clone, CLONE_PARENT | CLONE_PIDFD | SIGCHLD, 0, &pidfd, 0, 0);
            }
            if (pid == 0) {
                prctl(PR_SET_PDEATHSIG, SIGKILL);
                if (getppid() != wicket) _exit(126);
                if (req == 1) execl("/bin/cat", "cat", "/proc/self/cgroup", NULL);
                else execl("/bin/true", "true", NULL);
                _exit(127);
            }
            if (pid < 0) { int e = -errno; send_fd(pair[1], e, -1); continue; }
            if (cgroup >= 0) close(cgroup);
            send_fd(pair[1], (int)pid, pidfd);
            close(pidfd);
        }
    }
    close(pair[1]);
    int cgroup = -1;
    if (use_cgroup) { snprintf(path, sizeof path, "%s/vm-0", dir); cgroup = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC); }
    for (int i = 0; i < 6; i++) {
        long long s = now_us();
        int req = i == 0 ? 1 : 0;
        send_fd(pair[0], req, cgroup);
        int pid, pidfd;
        recv_fd(pair[0], &pid, &pidfd);
        long long e = now_us();
        if (pid < 0) { fprintf(stderr, "spawn failed: %s\n", strerror(-pid)); return 1; }
        siginfo_t info = {0};
        int rc = waitid(P_PIDFD, pidfd, &info, WEXITED);
        long long w = now_us();
        fprintf(stderr, "spawned %d in %lldus; waitid rc=%d errno=%d si_pid=%d code=%d status=%d (+%lldus)\n",
                pid, e - s, rc, rc ? errno : 0, info.si_pid, info.si_code, info.si_status, w - e);
        close(pidfd);
        usleep(i < 3 ? 1000000 : 1000);
    }
    return 0;
}
