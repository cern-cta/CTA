#ifndef _CSEC_PLUGIN_LOADER_H
#define _CSEC_PLUGIN_LOADER_H

#include <Csec_struct.h>

#define PLUGIN_SHLIB_NAME_TEMPLATE "libCsec_plugin_%s%s.so.%s"

/**
 * Locates and load a security plugin for a given mechanism.
 */
int _Csec_load_plugin(Csec_protocol_t *protocol, 
		      Csec_plugin_t **plugin);

/**
 * Locates and load a security plugin for a given mechanism.
 */
int _Csec_load_pluginExt(Csec_protocol_t *protocol, 
		      Csec_plugin_t **plugin,
		      int thread_safe);


/**
 * Unloads a security plugin and cleanup the structure.
 */
int _Csec_unload_plugin(Csec_plugin_t *plugin);
		     

#endif /* _CSEC_PLUGIN_LOADER_H */
