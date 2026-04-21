/*
 * memory_hog.c - Memory pressure workload for soft / hard limit testing.
 *
 * Default behavior:
 *   - allocate 8 MiB every second
 *   - touch each page so RSS actually grows
 *
 * Usage:
 *   /memory_hog [chunk_mb] [sleep_ms]
 *
 * If you plan to copy this binary into an Alpine rootfs, build it in a way
 * that is runnable inside that filesystem, such as static linking or
 * rebuilding it from inside the rootfs/toolchain you choose.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static size_t parse_size_mb(const char *arg, size_t fallback)
{
    char *end = NULL;
    unsigned long value = strtoul(arg, &end, 10); //This tries to parse the chunk size in megabytes from the command line argument. If parsing fails, it will return the fallback value.

    if (!arg || *arg == '\0' || (end && *end != '\0') || value == 0)
        return fallback;
    return (size_t)value;
}

static useconds_t parse_sleep_ms(const char *arg, useconds_t fallback)
{
    char *end = NULL;
    unsigned long value = strtoul(arg, &end, 10); //This tries to parse the sleep time in milliseconds from the command line argument. If parsing fails, it will return the fallback value.

    if (!arg || *arg == '\0' || (end && *end != '\0'))
        return fallback;
    return (useconds_t)(value * 1000U);
}

int main(int argc, char *argv[])
{
    const size_t chunk_mb = (argc > 1) ? parse_size_mb(argv[1], 8) : 8; //This sets the default chunk size to 8 MiB, but allows the user to specify a different chunk size in megabytes as a command line argument.
    const useconds_t sleep_us = (argc > 2) ? parse_sleep_ms(argv[2], 1000U) : 1000U * 1000U; //This sets the default sleep time to 1000 milliseconds (1 second), but allows the user to specify a different sleep time in milliseconds as a command line argument.
    const size_t chunk_bytes = chunk_mb * 1024U * 1024U; //This converts the chunk size from megabytes to bytes.
    int count = 0;

    while (1) {
        char *mem = malloc(chunk_bytes);
        if (!mem) {
            printf("malloc failed after %d allocations\n", count);
            break;
        }

        memset(mem, 'A', chunk_bytes); //This touches each page of the allocated memory to ensure that it is actually resident in RAM and contributes to the RSS of the process.
        count++;
        printf("allocation=%d chunk=%zuMB total=%zuMB\n",
               count, chunk_mb, (size_t)count * chunk_mb);
        fflush(stdout);
        usleep(sleep_us);
    }

    return 0;
}
