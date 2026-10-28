#ifndef COMMANDS_H
#define COMMANDS_H

#include "client.h"

void command(Client *client, char **args, const size_t *lens, size_t count);
void client_free_transaction_state(Client *client);

#endif
