#ifndef _CSEC_COMMON_H
#define _CSEC_COMMON_H

#include <marshall.h>
#include <Csec_struct.h>
#include <Csec_api.h>
#include <Csec_plugin_loader.h>




/* *********************************************** */
/* Csec error and apiinit                          */
/*                                                 */
/* *********************************************** */

int *C__Csec_errno();
#define Csec_errno (*C__Csec_errno())
int _Csec_errmsg(char *func, char *msg, ...);
int _Csec_apiinit(struct Csec_api_thread_info **thip);
void _Csec_sync_error(Csec_plugin_t *plugin);

/* *********************************************** */
/* Csec_context_t Utilities                        */
/*                                                 */
/* *********************************************** */

Csec_context_t *_Csec_create_context(enum Csec_context_type type,
				     enum Csec_service_type service_type);
Csec_context_t *_Csec_init_context(Csec_context_t *ctx, 
				   enum Csec_context_type type,
				   enum Csec_service_type service_type);
void _Csec_delete_context(Csec_context_t *ctx);
void _Csec_clear_context(Csec_context_t *ctx);
int _Csec_check_context(Csec_context_t *ctx, char *func);
#define CHECKCTX(CTX,FUNC,RET) if (_Csec_check_context(CTX,FUNC)!=0) { return RET; }

/* *********************************************** */
/* Csec_protocol_t Utilities                       */
/*                                                 */
/* *********************************************** */

Csec_protocol_t *_Csec_create_protocol(const char *prot);
Csec_protocol_t *_Csec_init_protocol(Csec_protocol_t *protocol,
				     const char *name);
Csec_protocol_t *_Csec_dup_protocol(const Csec_protocol_t *prot);
void _Csec_delete_protocol(Csec_protocol_t *prot);
char *_Csec_protocol_name(const Csec_protocol_t *prot);
int _Csec_protocol_cmp(const Csec_protocol_t *prot1,
		       const Csec_protocol_t *prot2);

/* *********************************************** */
/* Csec_id_t Utilities                             */
/*                                                 */
/* *********************************************** */

Csec_id_t *_Csec_create_id(const char *mech, const char *name);
Csec_id_t *_Csec_dup_id(const Csec_id_t *id);
void _Csec_delete_id(Csec_id_t *id);
char * _Csec_id_mech(const Csec_id_t *id);
char * _Csec_id_name(const Csec_id_t *id);

/* *********************************************** */
/* TRACE Utilities                                 */
/*                                                 */
/* *********************************************** */

int _Csec_setup_trace();
int _Csec_trace(char *func, char *msg, ...);






/* *********************************************** */
/* Csec tokens/IO  Utilities                       */
/*                                                 */
/* *********************************************** */

int _Csec_send_token(int s, 
		     csec_buffer_t tok, 
		     int timeout, 
		     int token_type);
int _Csec_recv_token(int s, 
		     csec_buffer_t tok, 
		     int timeout, 
		     int *rtype);
void _Csec_print_token(csec_buffer_t tok);  

void _Csec_clear_token(csec_buffer_t tok);


/* *********************************************** */
/* Protocol Utilities                              */
/*                                                 */
/* *********************************************** */



/**
 * Returns the list of protocols available to the client.
 */
int _Csec_client_lookup_protocols(Csec_protocol_t ***protocols, 
				  int *nbprotocols);

/**
 * Creates the list of authorized protocols for a given client, possibly depending
 * on its IP address. Currently doesn't support differentiating the protocol lists
 * by address.
 */
int _Csec_server_lookup_protocols(long client_address,
				 Csec_protocol_t ***protocols,
				 int *nbprotocols);

/**
 * Initializes the protocols in the context from a list rather than
 * from the environment variables.
 */
int _Csec_initialize_protocols_from_list(Csec_protocol_t *list,
					 Csec_protocol_t ***protocols, 
					 int *nbprotocols);


/* *********************************************** */
/* Service name utilities                          */
/*                                                 */
/* *********************************************** */

/**
 * Returns the the local host name and domain
 */
int _Csec_get_local_host_domain(char **host,
				char **domain);

/**
 * Returns the host/domain name of the connected peer
 */
int _Csec_get_peer_host_domain(int s,
			       char **host,
			       char **domain);


/* *********************************************** */
/* uid/gid lookup                                  */
/*                                                 */
/* *********************************************** */

int _Csec_name2id(char *name, uid_t *uid, uid_t *gid);


#endif /* _CSEC_COMMON_H */
