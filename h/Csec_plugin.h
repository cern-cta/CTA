#ifndef _CSEC_PLUGIN_H
#define _CSEC_PLUGIN_H

#include <Csec_common.h>

#define CSEC_METHOD_NAME_X(METH, MECH) METH ## MECH
#define CSEC_METHOD_NAME(METH, MECH) CSEC_METHOD_NAME_X(METH, MECH)

#define CSEC_DECLARE_FUNCTIONS(MECH) \
int DLL_DECL  Csec_init_context_ ## MECH _PROTO ((Csec_context_t *ctx)); \
int DLL_DECL  Csec_reinit_context_ ## MECH _PROTO ((Csec_context_t *ctx)); \
int DLL_DECL  Csec_delete_connection_context_ ## MECH _PROTO ((Csec_context_t *)); \
int DLL_DECL  Csec_delete_creds_ ## MECH _PROTO ((Csec_context_t *)); \
int DLL_DECL  Csec_acquire_creds_ ## MECH _PROTO ((Csec_context_t *ctx, \
					       char *service_name, \
					       int is_client)); \
int DLL_DECL  Csec_server_establish_context_ ## MECH _PROTO ((Csec_context_t *ctx, \
							  int s)); \
int DLL_DECL  Csec_server_establish_context_ext_ ## MECH _PROTO ((Csec_context_t *ctx, \
							      int s, \
							      char *buf, \
							      int len)); \
int DLL_DECL  Csec_client_establish_context_ ## MECH _PROTO ((Csec_context_t *ctx, \
							  int s)); \
int DLL_DECL Csec_map2name_ ## MECH _PROTO((Csec_context_t *ctx, \
					char *principal, \
					char *name, \
					int maxnamelen)); \
int DLL_DECL Csec_get_service_name_ ## MECH _PROTO ((Csec_context_t *ctx, \
						 int service_type, \
						 char *host, \
						 char *domain, \
						 char *service_name, \
						 int service_namelen));

CSEC_DECLARE_FUNCTIONS(KRB4)
CSEC_DECLARE_FUNCTIONS(KRB5)
CSEC_DECLARE_FUNCTIONS(GSI)
CSEC_DECLARE_FUNCTIONS(ID)

#endif /* Csec_plugin.h */
