#pragma once

/* this file is for internal use only by the core module */

#include <stdbool.h>

struct event_base;

struct context {
    struct event_base *evb;
    int timeout;
};

static bool admin_init;
static bool server_init;
static bool worker_init;
