#ifndef _CSEC_PLUGIN_H
#define _CSEC_PLUGIN_H

#include <Csec_common.h>

int DLL_DECL  Csec_init_context_impl _PROTO ((Csec_context_t *ctx));
int DLL_DECL  Csec_reinit_context_impl _PROTO ((Csec_context_t *ctx));
int DLL_DECL  Csec_delete_connection_context_impl _PROTO ((Csec_context_t *));
int DLL_DECL  Csec_delete_creds_impl _PROTO ((Csec_context_t *));
int DLL_DECL  Csec_server_acquire_creds_impl _PROTO ((Csec_context_t *ctx,
                                                 char *service_name));
int DLL_DECL  Csec_server_establish_context_impl _PROTO ((Csec_context_t *ctx,
							  int s));
int DLL_DECL  Csec_server_establish_context_ext_impl _PROTO ((Csec_context_t *ctx,
							      int s,
							      char *buf,
							      int len));
int DLL_DECL  Csec_client_establish_context_impl _PROTO ((Csec_context_t *ctx,
							  int s));
int DLL_DECL Csec_map2name_impl _PROTO((Csec_context_t *ctx, 
					char *principal, 
					char *name, 
					int maxnamelen));


int DLL_DECL Csec_get_service_name_impl _PROTO ((Csec_context_t *ctx, 
						 int service_type, 
						 char *host, 
						 char *domain,
						 char *service_name, 
						 int service_namelen));
#endif /* Csec_plugin.h */
