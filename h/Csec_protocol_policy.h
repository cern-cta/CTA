#ifndef _CSEC_PROTOCOL_POLICY_H
#define _CSEC_PROTOCOL_POLICY_H

typedef struct Csec_protocol {
  char id[PROTID_SIZE+1];
} Csec_protocol;

int Csec_client_get_protocols(Csec_protocol **protocols, int *nbprotocols);
int Csec_server_get_client_authorized_protocols(long client_address,
						Csec_protocol **protocols,
						int *nbprotocols);

    
#endif /* _CSEC_PROTOCOL_POLICY_H */
