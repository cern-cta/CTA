#include <stdio.h>
#include <getacct.h>
char *getacct();

int main(int argc, char *argv[]) {
	char *buf;
	buf = getacct();
	fprintf(stderr,"account=%s",buf);
}
