#ifndef _CSEC_PLUGIN_H
#define _CSEC_PLUGIN_H

#include <Csec_common.h>

#define CSEC_METHOD_NAME_X(METH, MECH) METH ## MECH
#define CSEC_METHOD_NAME(METH, MECH) CSEC_METHOD_NAME_X(METH, MECH)

#define CSEC_DECLARE_FUNCTIONS(MECH)					\
  int DLL_DECL								\
  Csec_plugin_activate_ ## MECH _PROTO ((Csec_plugin_t *plugin));	\
  int DLL_DECL								\
  Csec_plugin_deactivate_ ## MECH _PROTO ((Csec_plugin_t *plugin));	\
  int DLL_DECL								\
  Csec_plugin_initContext_ ## MECH _PROTO ((void **plugin_ctx,		\
					    enum Csec_service_type  svc_type, \
					    enum Csec_context_type cxt_type, \
					    int options));				\
  int DLL_DECL								\
  Csec_plugin_clearContext_ ## MECH _PROTO ((void *plugin_ctx));	\
  int DLL_DECL								\
  Csec_plugin_acquireCreds_ ## MECH _PROTO ((void *credentials,	\
					     char *service_name,	\
					     enum Csec_context_type cxt_type)); \
  int DLL_DECL								\
  Csec_plugin_serverEstablishContextExt_ ## MECH _PROTO ((void *plugin_ctx, \
							  int s,	\
							  char *buf,	\
							  int len,	\
							  Csec_id_t **client_id));		\
  int DLL_DECL								\
  Csec_plugin_clientEstablishContext_ ## MECH _PROTO ((void *plugin_ctx, \
							  int s));	\
  int DLL_DECL								\
  Csec_plugin_map2name_ ## MECH _PROTO((Csec_id_t *user_id,		\
					Csec_id_t **mapped_id));	\
  int DLL_DECL								\
  Csec_plugin_servicetype2name_ ## MECH _PROTO ((enum  Csec_service_type  svc_type, \
						 char *host,		\
						 char *domain,		\
						 char *service_name,	\
						 int service_namelen )); \
  int DLL_DECL								\
  Csec_plugin_getErrorMessage_ ## MECH _PROTO ((int *error, char **message)); \
  int DLL_DECL								\
  Csec_plugin_wrap_ ## MECH _PROTO ((void *plugin_context,		\
				     csec_buffer_t message,		\
				     csec_buffer_t crypt));		\
  int DLL_DECL								\
  Csec_plugin_unwrap_ ## MECH _PROTO ((void *plugin_context,		\
				       csec_buffer_t crypt,		\
				       csec_buffer_t message));		\
  int DLL_DECL								\
  Csec_plugin_isIdService_ ## MECH _PROTO ((Csec_id_t *id));		\
  int DLL_DECL								\
  Csec_plugin_exportDelegatedCredentials(void *plugin_context,		\
					 csec_buffer_t buffer);


CSEC_DECLARE_FUNCTIONS(KRB4)
CSEC_DECLARE_FUNCTIONS(KRB5)
CSEC_DECLARE_FUNCTIONS(GSI)
CSEC_DECLARE_FUNCTIONS(GSI_pthr)
CSEC_DECLARE_FUNCTIONS(ID)

#endif /* Csec_plugin.h */
