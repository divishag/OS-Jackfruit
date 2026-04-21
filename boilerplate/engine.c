#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char rootfs[PATH_MAX];
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int pipe_read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_supervisor_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur != NULL) {
        if (strncmp(cur->id, id, CONTAINER_ID_LEN) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur = ctx->containers;
    while (cur != NULL) {
        if ((cur->state == CONTAINER_STARTING || cur->state == CONTAINER_RUNNING) &&
            strcmp(cur->rootfs, rootfs) == 0) {
            return 1;
        }
        cur = cur->next;
    }
    return 0;
}

static void append_response(control_response_t *res, const char *text)
{
    size_t used = strlen(res->message);
    size_t left = sizeof(res->message) - used - 1;
    if (left > 0)
        strncat(res->message, text, left);
}

static void format_time_string(time_t t, char *buf, size_t buf_sz)
{
    struct tm tm_val;
    localtime_r(&t, &tm_val);
    strftime(buf, buf_sz, "%Y-%m-%d %H:%M:%S", &tm_val);
}

static void free_containers(container_record_t *head)
{
    while (head != NULL) {
        container_record_t *next = head->next;
        free(head);
        head = next;
    }
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        int fd;
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            ssize_t written = write(fd, item.data, item.length);
            (void)written;
            close(fd);
        }
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_arg_t *prod = (producer_arg_t *)arg;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(prod->pipe_read_fd, buffer, sizeof(buffer))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, prod->container_id, sizeof(item.container_id) - 1);
        memcpy(item.data, buffer, (size_t)n);
        item.length = (size_t)n;
        if (bounded_buffer_push(prod->buffer, &item) != 0)
            break;
    }

    close(prod->pipe_read_fd);
    free(prod);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;

    if (sethostname(config->id, strlen(config->id)) != 0) {
        perror("sethostname");
        _exit(1);
    }

    if (config->nice_value != 0 && nice(config->nice_value) == -1 && errno != 0) {
        perror("nice");
    }

    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        _exit(1);
    }

    if (chdir("/") != 0) {
        perror("chdir");
        _exit(1);
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount proc");
        _exit(1);
    }

    if (dup2(config->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(config->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        _exit(1);
    }

    close(config->log_write_fd);

    execl("/bin/sh", "sh", "-c", config->command, NULL);
    perror("exec failed");
    _exit(1);
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *cur = ctx->containers;
        while (cur != NULL) {
            if (cur->host_pid == pid) {
                if (WIFEXITED(status)) {
                    cur->exit_code = WEXITSTATUS(status);
                    cur->exit_signal = 0;
                    if (cur->stop_requested)
                        cur->state = CONTAINER_STOPPED;
                    else
                        cur->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    cur->exit_code = -1;
                    cur->exit_signal = WTERMSIG(status);
                    if (cur->stop_requested)
                        cur->state = CONTAINER_STOPPED;
                    else
                        cur->state = CONTAINER_KILLED;
                }
                unregister_from_monitor(ctx->monitor_fd, cur->id, cur->host_pid);
                break;
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static void handle_signal(int sig)
{
    if (g_supervisor_ctx == NULL)
        return;

    if (sig == SIGINT || sig == SIGTERM) {
        g_supervisor_ctx->should_stop = 1;
        if (g_supervisor_ctx->server_fd >= 0)
            close(g_supervisor_ctx->server_fd);
    }
}

static int install_signal_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    if (sigaction(SIGINT, &sa, NULL) != 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) != 0)
        return -1;

    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static int start_container(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *res)
{
    int pipefd[2];
    child_config_t *config = NULL;
    void *stack = NULL;
    producer_arg_t *prod = NULL;
    pthread_t producer_tid;
    int producer_created = 0;
    container_record_t *record = NULL;
    pid_t pid;

    pipefd[0] = -1;
    pipefd[1] = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        res->status = 1;
        snprintf(res->message, sizeof(res->message), "container id '%s' already exists", req->container_id);
        return -1;
    }
    if (rootfs_in_use_locked(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        res->status = 1;
        snprintf(res->message, sizeof(res->message), "rootfs already in use by a running container");
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) != 0) {
        res->status = errno;
        snprintf(res->message, sizeof(res->message), "pipe failed: %s", strerror(errno));
        return -1;
    }

    config = calloc(1, sizeof(*config));
    stack = malloc(STACK_SIZE);
    record = calloc(1, sizeof(*record));
    prod = calloc(1, sizeof(*prod));
    if (config == NULL || stack == NULL || record == NULL || prod == NULL) {
        res->status = ENOMEM;
        snprintf(res->message, sizeof(res->message), "memory allocation failed");
        goto fail;
    }

    strncpy(config->id, req->container_id, sizeof(config->id) - 1);
    strncpy(config->rootfs, req->rootfs, sizeof(config->rootfs) - 1);
    strncpy(config->command, req->command, sizeof(config->command) - 1);
    config->nice_value = req->nice_value;
    config->log_write_fd = pipefd[1];

    pid = clone(child_fn,
                (char *)stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                config);
    if (pid < 0) {
        res->status = errno;
        snprintf(res->message, sizeof(res->message), "clone failed: %s", strerror(errno));
        goto fail;
    }

    close(pipefd[1]);
    pipefd[1] = -1;

    memset(record, 0, sizeof(*record));
    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    record->host_pid = pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    record->stop_requested = 0;
    strncpy(record->rootfs, req->rootfs, sizeof(record->rootfs) - 1);
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);
    record = NULL;

    if (register_with_monitor(ctx->monitor_fd,
                              req->container_id,
                              pid,
                              req->soft_limit_bytes,
                              req->hard_limit_bytes) != 0) {
        snprintf(res->message, sizeof(res->message),
                 "warning: container started but monitor registration failed: %s",
                 strerror(errno));
    }

    prod->pipe_read_fd = pipefd[0];
    prod->buffer = &ctx->log_buffer;
    strncpy(prod->container_id, req->container_id, sizeof(prod->container_id) - 1);

    if (pthread_create(&producer_tid, NULL, producer_thread, prod) != 0) {
        res->status = errno;
        snprintf(res->message, sizeof(res->message), "pthread_create failed for producer");
        goto fail_after_record;
    }
    producer_created = 1;
    pthread_detach(producer_tid);
    prod = NULL;

    if (res->message[0] == '\0') {
        snprintf(res->message, sizeof(res->message), "container %s started with pid %d", req->container_id, pid);
    }
    res->status = 0;

    free(config);
    free(stack);
    return 0;

fail_after_record:
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t **indirect = &ctx->containers;
    while (*indirect != NULL) {
        if (strncmp((*indirect)->id, req->container_id, CONTAINER_ID_LEN) == 0) {
            container_record_t *tmp = *indirect;
            *indirect = tmp->next;
            free(tmp);
            break;
        }
        indirect = &(*indirect)->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

fail:
    if (pipefd[0] >= 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);
    if (!producer_created)
        free(prod);
    free(record);
    free(config);
    free(stack);
    return -1;
}

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *res)
{
    char line[512];
    char tbuf[64];
    container_record_t *cur;

    append_response(res, "ID\tPID\tSTATE\tSTARTED\tEXIT\tSIGNAL\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur != NULL) {
        format_time_string(cur->started_at, tbuf, sizeof(tbuf));
        snprintf(line,
                 sizeof(line),
                 "%s\t%d\t%s\t%s\t%d\t%d\n",
                 cur->id,
                 cur->host_pid,
                 state_to_string(cur->state),
                 tbuf,
                 cur->exit_code,
                 cur->exit_signal);
        append_response(res, line);
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (strlen(res->message) == strlen("ID\tPID\tSTATE\tSTARTED\tEXIT\tSIGNAL\n"))
        append_response(res, "<no containers>\n");
}

static void handle_logs(const control_request_t *req, control_response_t *res)
{
    char path[PATH_MAX];
    int fd;
    ssize_t n;
    size_t used = 0;
    char buf[512];

    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req->container_id);
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        res->status = errno;
        snprintf(res->message, sizeof(res->message), "cannot open log for %s: %s", req->container_id, strerror(errno));
        return;
    }

    memset(res->message, 0, sizeof(res->message));
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        size_t copy_n = (size_t)n;
        if (used + copy_n >= sizeof(res->message) - 1) {
            copy_n = sizeof(res->message) - used - 1;
        }
        memcpy(res->message + used, buf, copy_n);
        used += copy_n;
        if (used >= sizeof(res->message) - 1)
            break;
    }
    close(fd);
    res->status = 0;
}

static void handle_stop(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *res)
{
    container_record_t *rec;
    pid_t pid_to_kill;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_locked(ctx, req->container_id);
    if (rec == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        res->status = 1;
        snprintf(res->message, sizeof(res->message),
                 "unknown container id '%s'", req->container_id);
        return;
    }

    if (rec->state != CONTAINER_RUNNING && rec->state != CONTAINER_STARTING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        res->status = 1;
        snprintf(res->message, sizeof(res->message),
                 "container '%s' is not running", req->container_id);
        return;
    }

    rec->stop_requested = 1;
    pid_to_kill = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(pid_to_kill, SIGTERM) != 0) {
        res->status = errno;
        snprintf(res->message, sizeof(res->message),
                 "failed to stop '%s': %s", req->container_id, strerror(errno));
        return;
    }

    sleep(1);

    if (kill(pid_to_kill, 0) == 0) {
        if (kill(pid_to_kill, SIGKILL) != 0 && errno != ESRCH) {
            res->status = errno;
            snprintf(res->message, sizeof(res->message),
                     "failed to force-stop '%s': %s", req->container_id, strerror(errno));
            return;
        }
    }

    snprintf(res->message, sizeof(res->message),
             "stop requested for container %s", req->container_id);
    res->status = 0;
}

static void shutdown_running_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;
    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur != NULL) {
        if (cur->state == CONTAINER_RUNNING || cur->state == CONTAINER_STARTING) {
            pid_t pid = cur->host_pid;
            cur->stop_requested = 1;
            kill(pid, SIGTERM);
            sleep(1);
            if (kill(pid, 0) == 0)
                kill(pid, SIGKILL);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int handle_request(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *res)
{
    memset(res, 0, sizeof(*res));

    switch (req->kind) {
    case CMD_START:
    case CMD_RUN:
        return start_container(ctx, req, res);
    case CMD_PS:
        handle_ps(ctx, res);
        return 0;
    case CMD_LOGS:
        handle_logs(req, res);
        return 0;
    case CMD_STOP:
        handle_stop(ctx, req, res);
        return 0;
    default:
        res->status = 1;
        snprintf(res->message, sizeof(res->message), "unsupported command");
        return -1;
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int server_fd = -1;
    struct sockaddr_un addr;
    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST) {
        perror("mkdir logs");
        return 1;
    }

    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        fprintf(stderr, "failed to initialize log buffer\n");
        return 1;
    }
    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        fprintf(stderr, "failed to initialize metadata mutex\n");
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        fprintf(stderr, "failed to start logger thread\n");
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }

    if (install_signal_handlers() != 0) {
        fprintf(stderr, "failed to install signal handlers\n");
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        close(server_fd);
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }

    if (listen(server_fd, 8) != 0) {
        perror("listen");
        close(server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        pthread_mutex_destroy(&ctx.metadata_lock);
        bounded_buffer_destroy(&ctx.log_buffer);
        return 1;
    }

    ctx.server_fd = server_fd;
    g_supervisor_ctx = &ctx;

    printf("Supervisor running on %s\n", CONTROL_PATH);

    while (!ctx.should_stop) {
        int client_fd;
        control_request_t req;
        control_response_t res;
        ssize_t n;

        reap_children(&ctx);

        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            if (ctx.should_stop)
                break;
            perror("accept");
            continue;
        }

        n = read(client_fd, &req, sizeof(req));
        if (n == (ssize_t)sizeof(req)) {
            handle_request(&ctx, &req, &res);
            (void)write(client_fd, &res, sizeof(res));
        }
        close(client_fd);
        reap_children(&ctx);
    }

    shutdown_running_containers(&ctx);
    sleep(1);
    reap_children(&ctx);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (server_fd >= 0)
        close(server_fd);
    unlink(CONTROL_PATH);
    pthread_mutex_destroy(&ctx.metadata_lock);
    bounded_buffer_destroy(&ctx.log_buffer);
    free_containers(ctx.containers);
    g_supervisor_ctx = NULL;
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t res;
    ssize_t n;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(sock);
        return 1;
    }

    memset(&res, 0, sizeof(res));
    n = read(sock, &res, sizeof(res));
    if (n > 0)
        printf("%s\n", res.message);

    close(sock);
    return res.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
