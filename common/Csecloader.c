#include <stdio.h>
#include <Cdlopen_api.h>
#include <Csec_api.h>

#include "patchlevel.h"

static int (*Cclient_initContext)(Csec_context_t *, int,Csec_protocol *);
static int (*Cclient_establishContext)(Csec_context_t *,int);
static int (*Cserver_initContext)(Csec_context_t *,int,Csec_protocol *);
static int (*CclearContext)(Csec_context_t *);
static int (*Cserver_establishContext)(Csec_context_t *,int);
static int (*CgetClientId)(Csec_context_t *, char **, char **);
static int (*CmapUser)(const char *, const char *, char *, size_t, uid_t *, gid_t *);
char *error;

static int loaded = 0;
static void *handle;
  
int loader(){
  char filename[CA_MAXNAMELEN];

  if (loaded == 1)
    return 1; // is already loaded
  
  snprintf(filename, CA_MAXNAMELEN, "libcastorsecurity.so.%d.%d",
           MAJORVERSION, MINORVERSION);

  handle = Cdlopen (filename, RTLD_LAZY);
  if (!handle) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  Cdlerror(); // Clear any existing error 
  *(void **) (&Cclient_initContext) = Cdlsym(handle, "Csec_client_initContext");
  if (!Cclient_initContext) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&Cserver_initContext) = Cdlsym(handle, "Csec_server_initContext");
  if (!Cserver_initContext) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&Cclient_establishContext) = Cdlsym(handle, "Csec_client_establishContext");
  if (!Cclient_establishContext) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&Cserver_establishContext) = Cdlsym(handle, "Csec_server_establishContext");
  if (!Cserver_establishContext) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&CclearContext) = Cdlsym(handle, "Csec_clearContext");
  if (!CclearContext) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&CgetClientId) = Cdlsym(handle, "Csec_server_getClientId");
  if (!CgetClientId) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }
  *(void **) (&CmapUser) = Cdlsym(handle, "Csec_mapToLocalUser");
  if (!CmapUser) {
    fprintf (stderr, "%s\n", Cdlerror());
    return -1;
  }

  loaded = 1;
  return 0;
}

int getServer_initContext(Csec_context_t * security_context, int service_type, Csec_protocol * protocol){
  return (*Cserver_initContext)(security_context, service_type, protocol);
}

int getClient_initContext(Csec_context_t *security_context, int service_type,Csec_protocol *protocol){
  return (*Cclient_initContext)(security_context, service_type , protocol);
}

int getClient_establishContext(Csec_context_t *security_context ,int sock){
  return(*Cclient_establishContext)(security_context,sock);
}

int getServer_establishContext(Csec_context_t *security_context ,int sock){
  return (*Cserver_establishContext)(security_context,sock);
}

int getClientId(Csec_context_t *security_context , char **mech, char **name){
  return (*CgetClientId)(security_context, mech, name);
}

int getMapUser(const char *mech, const char *principal, char *username, size_t usernamesize, uid_t *euid, gid_t *egid){ 
  return (*CmapUser)(mech,principal, username,usernamesize, euid,egid);
}

int getClearContext(Csec_context_t *security_context){
  return  (*CclearContext)(security_context);
}

