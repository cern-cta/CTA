#include <sys/types.h>
#include "stage.h"
main(argc, argv)
int argc;
char **argv;
{
	int fun = 21;
	char path[256];
	int req_type = STAGEWRT;

	build_linkname (argv[1], path, sizeof(path), req_type);
	printf ("linkname=%s\n", path);
	build_Upath (fun, path, sizeof(path), req_type);
	printf ("Upath=%s\n", path);
}
