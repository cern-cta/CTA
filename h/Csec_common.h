#ifndef _CSEC_COMMON_H
#define _CSEC_COMMON_H

#include <osdep.h>
#include <Castor_limits.h>
#include <marshall.h>

#if defined(KRB5) && defined(linux) 
#include <gssapi/gssapi_generic.h>
#define GSS_C_NT_USER_NAME (gss_OID)gss_nt_user_name
#else
#if defined(KRB5) && !defined(linux)
#include <gssapi/gssapi.h>
#else
#if defined(GSI)
#include <globus_gss_assist.h>
#else
/* In all cases, we use the gss_buffer_t as the structure to hold the tokens */
typedef struct gss_buffer_desc_struct
{
  size_t length;
  void *value;
} gss_buffer_desc, *gss_buffer_t;
#endif
#endif
#endif

#define ERRBUFSIZE 2000
#define HEADBUFSIZE 20

int *C__Csec_errno();
#define Csec_errno (*C__Cns_errno())

/** Structure with thread specific information */
struct Csec_api_thread_info {
  char *errbufp;
  int	errbuflen;
  int	sec_errno;
  int   init_done;
  int   trace_mode;
  char  trace_file[CA_MAXNAMELEN];
};

/* Magic number for authentication tokens */
#define CSEC_TOKEN_MAGIC_1   0xCA03

/** Various token types */
#define CSEC_TOKEN_TYPE_PROTOCOL_REQ   0x1
#define CSEC_TOKEN_TYPE_PROTOCOL_RESP  0x2
#define CSEC_TOKEN_TYPE_HANDSHAKE      0x3
#define CSEC_TOKEN_TYPE_DATA           0x4


/* char CSEC_TOKEN_TYPE[][30] = { "UNKNOWN",  */
/* 			       "PROTOCOL_REQ", */
/* 			       "PROTOCOL_RESP", */
/* 			       "HANDSHAKE", */
/* 			       "DATA", */
/* 			       "OOB" }; */

/** Buffer size for errors */
#define PRTBUFSZ 2000
/** Timeout used in the netread/netwrites */
#define CSEC_NET_TIMEOUT 10000

/** Environment variables to set to use Csec_trace */
#define CSEC_TRACE "CSEC_TRACE"
#define CSEC_TRACEFILE "CSEC_TRACEFILE"

/** Environment variable to switch mechanism */
#define CSEC_MECH "CSEC_MECH"

#define PROTID_SIZE 20

#define CA_MAXSERVICENAMELEN 512

#define CSEC_SERVICE_TYPE_HOST    0
#define CSEC_SERVICE_TYPE_CENTRAL 1
#define CSEC_SERVICE_TYPE_DISK    2
#define CSEC_SERVICE_TYPE_TAPE    3

#define PROT_STAT_INITIAL       0x0
#define PROT_STAT_REQ_SENT      0x1
#define PROT_STAT_NOK           0x2
#define PROT_STAT_OK            0x4

#define PROT_REQ_OK "OK"
#define PROT_REQ_NOK "NOK"

typedef struct Csec_protocol {
  char id[PROTID_SIZE+1];
} Csec_protocol;

/** Structure holding the context */ 
typedef struct Csec_context {
  U_LONG flags; /* Flags containing status of  context structure */
  /* char protid[PROTID_SIZE+1]; */    /* Protocol for which the context has been initialized */
  void *shhandle; /* Handle to the shared library */
  void *credentials; /*     gss_cred_id_t credentials; */
  void *connection_context;     /*     gss_ctx_id_t context; */

  /* Variables containing ths status of the security protocol negociation
     between client and server */
  int protocol_negociation_status;
  Csec_protocol *protocols; /* Contains the list of protocols supported by this end of the connection */
  int nb_protocols; /* Number of entries in the previous list */
  int current_protocol; /* Index of the protocol in the list mentioned above */
  Csec_protocol *peer_protocols; /* List of protocols accepted by peer */
  int nb_peer_protocols; /* Number of entries in list above */

  /* Local and peer identity information */
  char local_name[CA_MAXCSECNAMELEN+1];
  char peer_name[CA_MAXCSECNAMELEN+1]; /*  Requested server name (in the Csec client) */
  char effective_peer_name[CA_MAXCSECNAMELEN+1]; /* Name returned by the GSI layer */
  unsigned long context_flags; /* Context flags from the GSS API */
  char peer_username[CA_MAXCSECNAMELEN+1];
  int peer_uid;
  int peer_gid;

  /* Used on the server only ! */
  int server_service_type;

  int (*Csec_init_context)(struct Csec_context *);
  int (*Csec_reinit_context)(struct Csec_context *);
  int (*Csec_delete_connection_context)(struct Csec_context *);
  int (*Csec_delete_creds)(struct Csec_context *);
  int (*Csec_server_acquire_creds)(struct Csec_context *, char *);
  int (*Csec_server_establish_context)(struct Csec_context *, int);
  int (*Csec_server_establish_context_ext)(struct Csec_context *, int,char *, int);
  int (*Csec_client_establish_context)(struct Csec_context *, int);
  int (*Csec_map2name)(struct Csec_context *, char *, char *, int);
  int (*Csec_get_service_name)(struct Csec_context *, int, char *, char *, char *, int);
  
} Csec_context;

#define CSEC_CTX_INITIALIZED          0x00000001L
#define CSEC_CTX_CREDENTIALS_LOADED   0x00000002L
#define CSEC_CTX_CONTEXT_ESTABLISHED  0x00000004L
#define CSEC_CTX_SERVICE_TYPE_SET     0x00000008L
#define CSEC_CTX_PROTOCOL_LOADED      0x00000010L
#define CSEC_CTX_SHLIB_LOADED         0x00000020L
#define CSEC_CTX_SERVICE_NAME_SET     0x00000040L
#define CSEC_CTX_USER_MAPPED          0x00000080L

int DLL_DECL check_ctx _PROTO ((Csec_context *, char *));
void DLL_DECL *Csec_get_shlib _PROTO ((Csec_context *)); 
int DLL_DECL Csec_init_context_ext _PROTO ((Csec_context *, int, int)) ;
/*int DLL_DECL Csec_init_protocol _PROTO ((Csec_context *, char *));*/
int DLL_DECL Csec_errmsg _PROTO((char *func, char *msg, ...));
int DLL_DECL Csec_apiinit _PROTO((struct Csec_api_thread_info **thip));
int DLL_DECL Csec_seterrbuf _PROTO((char *buffer, int buflen));
int DLL_DECL Csec_errmsg _PROTO((char *func, char *msg, ...));
int DLL_DECL Csec_trace _PROTO((char *func, char *msg, ...));
int DLL_DECL Csec_setup_trace _PROTO(());
int DLL_DECL _Csec_recv_token _PROTO ((int s, gss_buffer_t tok, int timeout, int *token_type));
int DLL_DECL _Csec_send_token _PROTO ((int s, gss_buffer_t tok, int timeout, int token_type));
void DLL_DECL  _Csec_print_token _PROTO((gss_buffer_t tok));

int DLL_DECL Csec_client_negociate_protocol _PROTO((int, int, Csec_context *));
int DLL_DECL Csec_client_send_protocol _PROTO((int, int, Csec_context *));
int DLL_DECL Csec_client_receive_server_reponse _PROTO((int, int, Csec_context *));
int DLL_DECL Csec_server_receive_protocol _PROTO((int, int, Csec_context *, char *, int));
int DLL_DECL Csec_server_send_response _PROTO ((int, int, Csec_context *, int));   


#endif /* _CSEC_COMMON_H */
