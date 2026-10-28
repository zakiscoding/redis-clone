#ifndef LOOP_H
#define LOOP_H

#include "platform.h"

socket_t server_listen(int port);
void run_loop(socket_t listener);

#endif
