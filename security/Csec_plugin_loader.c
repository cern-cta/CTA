/*
 * $Id: Csec_plugin_loader.c,v 1.1 2005/03/15 22:52:37 bcouturi Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_plugin_loader.c - Functions to load/unload the castor security plugins
 */


#include <dlfcn.h>
#include <stdlib.h>
#include <serrno.h>
#include <errno.h>
#include <stdio.h>
#include <Csec_common.h>
#include <Csec_plugin_loader.h>


/* Macro to initialize one symbol in the context structure */
#define DLSETFUNC(CTX, HDL, SYM, SFX) strcpy(symname, #SYM "_");	\
  strcat(symname, _Csec_protocol_name(CTX->protocol));			\
  strcat(symname, SFX);							\
  if ((CTX->SYM = dlsym(HDL, symname)) == NULL) {			\
    serrno =  ESEC_NO_SECMECH;						\
    _Csec_errmsg(func, "Error finding symbol %s: %s",			\
		#SYM, dlerror());					\
    CTX->shhandle = NULL;						\
    return -1; }


/**
 * Unloads a security plugin and cleanup the structure.
 */
int _Csec_unload_plugin(Csec_plugin_t *plugin) {
  char *func = "Csec_unload_plugin";

  _Csec_trace(func, "Unloading plugin\n");
  
  if (plugin == NULL) {
    return 0;
  }

  if (plugin->Csec_plugin_deactivate != NULL) {
    plugin->Csec_plugin_deactivate(plugin);
  }

  if (plugin->shhandle != NULL) {
    dlclose(plugin->shhandle);
    plugin->shhandle = NULL;
  }

  plugin->Csec_plugin_activate = NULL;
  plugin->Csec_plugin_deactivate = NULL;
  plugin->Csec_plugin_initContext = NULL;
  plugin->Csec_plugin_clearContext = NULL;
  plugin->Csec_plugin_acquireCreds = NULL;
  plugin->Csec_plugin_serverEstablishContextExt = NULL;
  plugin->Csec_plugin_clientEstablishContext = NULL;
  plugin->Csec_plugin_map2name = NULL;
  plugin->Csec_plugin_servicetype2name = NULL;
  plugin->Csec_plugin_getErrorMessage = NULL;
  plugin->Csec_plugin_isIdService = NULL;
  plugin->Csec_plugin_wrap = NULL;
  plugin->Csec_plugin_unwrap = NULL;
  plugin->Csec_plugin_exportDelegatedCredentials = NULL;
  free(plugin);
  return 0;
}



/**
 * Locates and load a security plugin for a given mechanism.
 */
int _Csec_load_plugin(Csec_protocol_t *protocol, 
		      Csec_plugin_t **plugin) {
  
#ifdef CSEC_PTHREAD
  return _Csec_load_pluginExt(protocol, plugin, 1);
#else 
  return _Csec_load_pluginExt(protocol, plugin, 0);
#endif;

}



/**
 * Locates and load a security plugin for a given mechanism.
 */
int _Csec_load_pluginExt(Csec_protocol_t *protocol, 
			 Csec_plugin_t **plugin,
			 int thread_safe) {

  char filename[CA_MAXNAMELEN];
  char suffix[CA_MAXNAMELEN];
  char *threaded = "_thread";
  char symname[256];
  void *handle;
  Csec_plugin_t *tmp;
  char *func = "Csec_load_plugin";
  
  _Csec_trace(func, "Loading plugin\n");

  /* Checking input parameters */
  if (plugin == NULL || protocol == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "protocol or plugin NULL");
    return -1;
  }

  /* Allocating the plugin structure on the heap */
  *plugin = calloc(sizeof(Csec_plugin_t), 1);
  if (plugin == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for plugin structure");
    return -1;
  }

  tmp = *plugin;

  tmp->thread_safe = thread_safe;
  tmp->protocol = _Csec_dup_protocol(protocol);
  if (tmp->protocol == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for plugin protocol");
    return -1;
  }

  /* Setup the symbol suffix */
  suffix[0] = '\0';
  if (strcmp(_Csec_protocol_name(protocol),"GSI") == 0 
      && thread_safe) {
    strcpy(suffix,"_pthr");
  }

  /* Creating the library name */
  snprintf(filename, 
	   CA_MAXNAMELEN, 
	   PLUGIN_SHLIB_NAME_TEMPLATE, 
	   _Csec_protocol_name(protocol),
	   thread_safe?threaded:"",
	   CSEC_PLUGIN_LIBSUFFIX);
  
  _Csec_trace(func, "Using shared library <%s> for mechanism <%s>\n",
	      filename,
	      _Csec_protocol_name(protocol));
    
  /* Opening the shared library */
  handle = dlopen(filename, RTLD_NOW);
  
  /* Duplicate dlerrmsg message as it can only be read once */
  if (handle == NULL) {
    char dlerrmsg[ERRBUFSIZE+1];
    serrno =  ESEC_NO_SECMECH;
    strncpy(dlerrmsg, dlerror(), ERRBUFSIZE);
    
    tmp->shhandle = NULL;
    _Csec_trace(func, "Error opening shared library %s: %s\n", filename,
		dlerrmsg);
    _Csec_errmsg(func, "Error opening shared library %s: %s\n", filename,
		 dlerrmsg);
    return -1;
  }

  tmp->shhandle = handle;

  DLSETFUNC(tmp, handle, Csec_plugin_activate, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_deactivate, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_initContext, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_clearContext, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_serverEstablishContextExt, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_clientEstablishContext, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_map2name, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_servicetype2name, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_getErrorMessage, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_isIdService, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_wrap, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_unwrap, suffix);
  DLSETFUNC(tmp, handle, Csec_plugin_exportDelegatedCredentials, suffix);

  if (tmp->Csec_plugin_activate(tmp) <0) {
    _Csec_unload_plugin(tmp);
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Error calling activate method");
    return -1;
  }

  return 0;
    
}
