#pragma once

#include <Csec_common.h>
#define CSEC_DEFAULT_MECHS "KRB5"

int Csec_client_lookup_protocols(Csec_protocol **protocols, int *nbprotocols);
int Csec_server_lookup_protocols(long client_address,
			      Csec_protocol **protocols,
			      int *nbprotocols);
int Csec_server_set_protocols(Csec_context_t *ctx,
			      int socket);
int Csec_initialize_protocols_from_list(Csec_context_t *ctx,
                                        Csec_protocol *protocol);
    
