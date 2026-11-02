// /init of the VMs of disk-measure.sh: mounts /dev/vdb at /mnt and runs what the kernel's
// command line names (disk=<scenario>[:<n>]), printing to the serial console. Powers the VM
// off at the end. Static: musl-gcc -static -O2.
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/reboot.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <time.h>
#include <unistd.h>

#define EXT4_IOC_RESIZE_FS _IOW('f', 16, uint64_t)

static double now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e3 + t.tv_nsec / 1e6;
}

static void die(const char *what) {
    printf("FAILED %s: %s\n", what, strerror(errno));
    fflush(stdout);
    sync();
    reboot(RB_AUTOBOOT);
    _exit(1);
}

static int compare(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

// The cf-api's save of its state: a file written whole, by a rename.
static void save(const char *body, size_t bytes, int durable) {
    int fd = open("/mnt/state.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) die("open state.tmp");
    if (write(fd, body, bytes) != (ssize_t)bytes) die("write state.tmp");
    if (durable && fsync(fd)) die("fsync state.tmp");
    close(fd);
    if (rename("/mnt/state.tmp", "/mnt/state")) die("rename");
    if (durable) {
        int dir = open("/mnt", O_RDONLY | O_DIRECTORY);
        if (dir < 0 || fsync(dir)) die("fsync /mnt");
        close(dir);
    }
}

// disk=fsync:<rounds>: how long a durable save of 100 KiB takes.
static void scenario_fsync(int rounds) {
    static char body[100 * 1024];
    memset(body, 'x', sizeof body);
    double *took = calloc(rounds, sizeof *took);
    for (int i = 0; i < rounds; i++) {
        double started = now_ms();
        save(body, sizeof body, 1);
        took[i] = now_ms() - started;
    }
    qsort(took, rounds, sizeof *took, compare);
    printf("durable save of 100 KiB, %d rounds: p50 %.2fms p90 %.2fms p99 %.2fms max %.2fms\n",
           rounds, took[rounds / 2], took[rounds * 9 / 10], took[rounds * 99 / 100],
           took[rounds - 1]);
}

// disk=count:<durable>: a counter saved every 20ms, each printed once saved, until killed.
static void scenario_count(int durable) {
    for (unsigned long n = 1;; n++) {
        char line[32];
        int len = snprintf(line, sizeof line, "%lu\n", n);
        save(line, len, durable);
        printf("saved %lu\n", n);
        fflush(stdout);
        usleep(20000);
    }
}

// disk=read: what the counter is after the mount.
static void scenario_read(void) {
    char line[32] = "";
    int fd = open("/mnt/state", O_RDONLY);
    if (fd < 0) {
        printf("the counter reads nothing: no file\n");
        return;
    }
    ssize_t got = read(fd, line, sizeof line - 1);
    close(fd);
    if (got <= 0) {
        printf("the counter reads nothing: an empty file\n");
    } else if (!line[0]) {
        printf("the counter reads nothing: %zd NUL bytes\n", got);
    } else {
        line[got - 1] = 0;
        printf("the counter reads %s\n", line);
    }
}

// disk=fill:<MiB>: that much written and synced, a line per 16 MiB.
static void scenario_fill(int mib) {
    static char chunk[1 << 20];
    memset(chunk, 'y', sizeof chunk);
    int fd = open("/mnt/fill", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) die("open fill");
    double started = now_ms();
    for (int i = 0; i < mib; i++) {
        if (write(fd, chunk, sizeof chunk) != sizeof chunk) die("write fill");
        if ((i + 1) % 16 == 0) {
            if (fsync(fd)) die("fsync fill");
            printf("written %d MiB after %.0fms\n", i + 1, now_ms() - started);
            fflush(stdout);
        }
    }
    if (fsync(fd)) die("fsync fill");
    printf("wrote %d MiB in %.0fms\n", mib, now_ms() - started);
    close(fd);
}

// disk=grow: the file system grown to the drive's size.
static void scenario_grow(void) {
    struct statfs before, after;
    if (statfs("/mnt", &before)) die("statfs");
    int fd = open("/dev/vdb", O_RDONLY);
    if (fd < 0) die("open /dev/vdb");
    off_t bytes = lseek(fd, 0, SEEK_END);
    close(fd);
    uint64_t blocks = bytes / before.f_bsize;
    int dir = open("/mnt", O_RDONLY | O_DIRECTORY);
    if (dir < 0) die("open /mnt");
    double started = now_ms();
    if (ioctl(dir, EXT4_IOC_RESIZE_FS, &blocks)) die("EXT4_IOC_RESIZE_FS");
    double took = now_ms() - started;
    if (statfs("/mnt", &after)) die("statfs");
    printf("grown from %lu to %lu MiB of blocks in %.1fms\n",
           (unsigned long)(before.f_blocks * before.f_bsize >> 20),
           (unsigned long)(after.f_blocks * after.f_bsize >> 20), took);
}

// disk=stop:<MiB>: that much left dirty, then what a stop would do: sync, and unmount.
static void scenario_stop(int mib) {
    static char chunk[1 << 20];
    memset(chunk, 'z', sizeof chunk);
    int fd = open("/mnt/dirty", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) die("open dirty");
    for (int i = 0; i < mib; i++) {
        if (write(fd, chunk, sizeof chunk) != sizeof chunk) die("write dirty");
    }
    close(fd);
    double started = now_ms();
    sync();
    double synced = now_ms() - started;
    if (umount("/mnt")) die("umount");
    printf("%d MiB dirty: sync %.1fms, unmount %.1fms more\n", mib, synced,
           now_ms() - started - synced);
}

int main(void) {
    double booted = now_ms();
    mount("proc", "/proc", "proc", 0, NULL);
    mount("sysfs", "/sys", "sysfs", 0, NULL);
    setvbuf(stdout, NULL, _IOLBF, 0);
    char cmdline[2048] = "";
    int fd = open("/proc/cmdline", O_RDONLY);
    if (fd >= 0) {
        ssize_t got = read(fd, cmdline, sizeof cmdline - 1);
        if (got > 0) cmdline[got] = 0;
        close(fd);
    }
    char *arg = strstr(cmdline, "disk=");
    char scenario[32] = "read";
    int n = 0;
    if (arg) sscanf(arg, "disk=%31[a-z]:%d", scenario, &n);
    double started = now_ms();
    // diskopts=<ext4's mount options>
    char options[128] = "";
    char *given = strstr(cmdline, "diskopts=");
    if (given) sscanf(given, "diskopts=%127s", options);
    if (mount("/dev/vdb", "/mnt", "ext4", MS_NOSUID | MS_NODEV, options)) die("mount /dev/vdb");
    printf("mounted in %.1fms, %.0fms after /init started\n", now_ms() - started,
           now_ms() - booted);
    if (!strcmp(scenario, "fsync")) scenario_fsync(n);
    else if (!strcmp(scenario, "count")) scenario_count(n);
    else if (!strcmp(scenario, "read")) scenario_read();
    else if (!strcmp(scenario, "fill")) scenario_fill(n);
    else if (!strcmp(scenario, "grow")) scenario_grow();
    else if (!strcmp(scenario, "stop")) scenario_stop(n);
    else printf("unknown scenario %s\n", scenario);
    if (!strcmp(scenario, "stop")) {
        printf("done\n");
    } else {
        double syncing = now_ms();
        sync();
        umount("/mnt");
        printf("done (sync and unmount %.1fms)\n", now_ms() - syncing);
    }
    fflush(stdout);
    reboot(RB_AUTOBOOT);
    return 0;
}
