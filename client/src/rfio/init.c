#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <rfio_api.h>
#include <Cns_api.h>
#include <stdarg.h>

int getipath_proxy(int fd, char *name, char *token)
{
	struct rfio_api_thread_info *thip;
	int (*f)(int, char *);
	
	if(rfio_apiinit(&thip) || thip->initialized == 0)
		return -1;

	fprintf(stderr, "DEBUG: in CASTOR2 getipath proxy...\n");
			
	f = thip->__rfio_HsmIf_getipath;
	return f(fd, name);
}

int reqtoput_proxy(char *name, char *token)
{
	struct rfio_api_thread_info *thip;
	int (*f)(char *);

	fprintf(stderr, "DEBUG: in CASTOR2 reqtoput proxy...\n");

	if(rfio_apiinit(&thip) || thip->initialized == 0)
		return -1;

	f = thip->__rfio_HsmIf_reqtoput;
	return f(name);
}

int plugin_init(struct rfio_api_thread_info *thip, void* libhandle)
{
	thip->rfio_parseln = dlsym(libhandle, "rfio_parseln");
	if (!thip->rfio_parseln) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->rfio_HsmIf_open_limbysz = dlsym(libhandle, "rfio_HsmIf_open_limbysz");
	if (!thip->rfio_HsmIf_open_limbysz) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->rfio_HsmIf_close = dlsym(libhandle, "rfio_HsmIf_close");
	if (!thip->rfio_HsmIf_close) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	/* getipath() and reqtoput() have to be libhandled in a special way through proxy
	 * functions, due to different prototypes in DPM and CASTOR2.
	 */
	thip->__rfio_HsmIf_getipath = dlsym(libhandle, "rfio_HsmIf_getipath");
	if (!thip->__rfio_HsmIf_getipath) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	thip->rfio_HsmIf_getipath = getipath_proxy;

	thip->__rfio_HsmIf_reqtoput = dlsym(libhandle, "rfio_HsmIf_reqtoput");
	if (!thip->__rfio_HsmIf_reqtoput) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	thip->rfio_HsmIf_reqtoput = reqtoput_proxy;
	
	thip->rfio_HsmIf_open = dlsym(libhandle, "rfio_HsmIf_open");
	if (!thip->rfio_HsmIf_open) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->rfio_HsmIf_reqtoput = dlsym(libhandle, "rfio_HsmIf_reqtoput");
	if (!thip->rfio_HsmIf_reqtoput) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->rfio_HsmIf_unlink = dlsym(libhandle, "rfio_HsmIf_unlink");
	if (!thip->rfio_HsmIf_unlink) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->rfio_copy = dlsym(libhandle, "rfio_copy");
	if (!thip->rfio_copy) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	thip->rfio_HsmIf_FirstWrite = dlsym(libhandle, "rfio_HsmIf_FirstWrite");
	if (!thip->rfio_HsmIf_FirstWrite) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	thip->rfio_HsmIf_IsCnsFile = dlsym(libhandle, "rfio_HsmIf_IsCnsFile");
	if (!thip->rfio_HsmIf_IsCnsFile) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	/* load in the nameserver API pointers */
	libhandle = dlopen("libshift.so", RTLD_NOW);
	if (!libhandle) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	thip->Cns_access = dlsym(libhandle, "Cns_access");
	if (!thip->Cns_access) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_chdir = dlsym(libhandle, "Cns_chdir");
	if (!thip->Cns_chdir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_chmod = dlsym(libhandle, "Cns_chmod");
	if (!thip->Cns_chmod) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_chown = dlsym(libhandle, "Cns_chown");
	if (!thip->Cns_chown) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_closedir = dlsym(libhandle, "Cns_closedir");
	if (!thip->Cns_closedir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_creat = dlsym(libhandle, "Cns_creat");
	if (!thip->Cns_creat) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_getcwd = dlsym(libhandle, "Cns_getcwd");
	if (!thip->Cns_getcwd) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_mkdir = dlsym(libhandle, "Cns_mkdir");
	if (!thip->Cns_mkdir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_opendir = dlsym(libhandle, "Cns_opendir");
	if (!thip->Cns_opendir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_stat = dlsym(libhandle, "Cns_stat");
	if (!thip->Cns_stat) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_readdir = dlsym(libhandle, "Cns_readdir");
	if (!thip->Cns_readdir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_readdirx = dlsym(libhandle, "Cns_readdirx");
	if (!thip->Cns_readdirx) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_rename = dlsym(libhandle, "Cns_rename");
	if (!thip->Cns_rename) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_rewinddir = dlsym(libhandle, "Cns_rewinddir");
	if (!thip->Cns_rewinddir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_rmdir = dlsym(libhandle, "Cns_rmdir");
	if (!thip->Cns_rmdir) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}

	thip->Cns_selectsrvr = dlsym(libhandle, "Cns_selectsrvr");
	if (!thip->Cns_selectsrvr) {
		fprintf(stderr, "%s\n", dlerror());
		return -1;
	}
	return 0;
}

