#ifndef _CSEC_STRUCT_H
#define _CSEC_STRUCT_H

#include <osdep.h>
#include <Castor_limits.h>
#include <Csec_constants.h>


/* Structure used in Csec - similar to a gss_buffer */
typedef struct csec_buffer_desc_struct
{
  size_t length;
  void *value;
} csec_buffer_desc, *csec_buffer_t;


/**
 * Structure describing a protocol
 */
typedef struct Csec_protocol {
  char *name;
} Csec_protocol_t;


/**
 *  Structure containing a user's identity
 */
typedef struct Csec_id {
  char *mech;
  char *name;
} Csec_id_t;


/**
 * Structure representing a the Csec plugin shared object.
 */
typedef struct Csec_plugin {

  Csec_protocol_t *protocol; /* The protocol supported by this plugin */
  
  void *shhandle; /* Handle to the shared library */
  int thread_safe;
  
  /* Pointers to the functions in the loaded shared libraries */
  int (*Csec_plugin_activate)(struct Csec_plugin *);
  int (*Csec_plugin_deactivate)(struct Csec_plugin *);
  int (*Csec_plugin_initContext)(void **,
				 enum Csec_service_type,
				 enum Csec_context_type,
				 int);
  int (*Csec_plugin_clearContext)(void *);
  int (*Csec_plugin_acquireCreds) (void *,
				   char *,
				   enum Csec_context_type);
  int (*Csec_plugin_serverEstablishContextExt)(void *,
					       int,
					       char *,
					       int,
					       Csec_id_t **);
  int (*Csec_plugin_clientEstablishContext)(void *,
					    int s);
  int (*Csec_plugin_map2name)(Csec_id_t *,		
			      Csec_id_t **);		
  int (*Csec_plugin_servicetype2name)(enum  Csec_service_type,
				     char *,
				     char *,
				     char *,
				     int);
  int (*Csec_plugin_getErrorMessage)(int *, char **); 
  int (*Csec_plugin_wrap)(void *,		
			  csec_buffer_t,		
			  csec_buffer_t);		
  int (*Csec_plugin_unwrap)(void *,		
			    csec_buffer_t,		
			    csec_buffer_t);		
  int (*Csec_plugin_isIdService)(Csec_id_t *id);
  int (*Csec_plugin_exportDelegatedCredentials)(void *,
						csec_buffer_t);

} Csec_plugin_t;


/** Structure holding the context */ 
typedef struct Csec_context {
  int magic;
  enum Csec_context_type context_type;
  enum Csec_service_type service_type;
  U_LONG flags; /* Flags containing status of  context structure */
  U_LONG options; /* Flag containing the options CSEC_OPT...*/ 

  Csec_plugin_t *plugin;
  void *plugin_context;

  /* Protocol negoiciation variables */
  int protocolNegociationStatus;
  Csec_protocol_t**supported_protocols;
  int nb_supported_protocols;
  Csec_protocol_t *protocol;
  int peer_protocol_version;
  int peer_options;
  Csec_protocol_t **peer_protocols; /* List of protocols offered by peer */
  int nb_peer_protocols; /* Number of entries in list above */

  /* Identity of local local and peer context */
  Csec_id_t *local_id;
  Csec_id_t *peer_id;
  Csec_id_t *authorization_id;
  char *peer_username;
  uid_t peer_uid;
  gid_t peer_gid;

  /* Cache for delegated credentials */
  csec_buffer_desc delegated_credentials;

} Csec_context_t;

/** 
 * Structure with thread specific information 
 */
struct Csec_api_thread_info {
  char  *errbuf;
  int   errbufsize;
  int	sec_errno;
  int   init_done;
  int   trace_mode;
  char  trace_file[CA_MAXNAMELEN+1];
  Csec_context_t default_context;
};

#endif /* _CSEC_STRUCT_H */
