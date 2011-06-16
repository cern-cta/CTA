/*
 * $Id: skiptape.c,v 1.13 2008/10/28 08:04:11 wiebalck Exp $
 */

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

int skiptpfff(int tapefd, char *path, int n) {
	char func[16];
	ENTRY (skiptpfff);
	RETURN (0);
}

int skiptpff(int tapefd, char *path, int n) {
	char func[16];
	ENTRY (skiptpff);
	RETURN (0);
}

int skiptpfb(int tapefd, char *path, int n) {
	char func[16];
	ENTRY (skiptpfb);
	RETURN (0);
}

int skiptprf(int tapefd, char *path, int n, int silent) {
	char func[16];
	ENTRY (skiptprf);
	RETURN (0);
}

int skiptprb(int tapefd, char *path, int n) {
	char func[16];
	ENTRY (skiptprb);
	RETURN (0);
}

int skip2eod(int tapefd, char *path) {
        char func[16];
	ENTRY (skip2eod);
	RETURN (0);
}
