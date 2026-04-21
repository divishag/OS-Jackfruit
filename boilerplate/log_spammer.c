#include <stdio.h>
#include <unistd.h>

int main() {
    int i = 1;
    while (1) {
        printf("stdout line %d\n", i);
        fprintf(stderr, "stderr line %d\n", i);
        fflush(stdout);
        fflush(stderr);
        i++;
        sleep(1);
    }
    return 0;
}
