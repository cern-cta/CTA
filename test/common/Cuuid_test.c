#include <stdlib.h>
#include <stdio.h>
#include "serrno.h"
#include "Cuuid.h"

Cuuid_t NameSpace_DNS = { /* 6ba7b810-9dad-11d1-80b4-00c04fd430c8 */
	(U_LONG) 0x6ba7b810,
	(U_SHORT) 0x9dad,
	(U_SHORT) 0x11d1,
	(U_BYTE) 0x80,
	(U_BYTE) 0xb4,
	(BYTE) 0x00,
	(BYTE) 0xc0,
	(BYTE) 0x4f,
	(BYTE) 0xd4,
	(BYTE) 0x30,
	(BYTE) 0xc8
};

/* puid -- print a UUID */
void puid _PROTO((Cuuid_t));


int main(argc,argv)
     int argc;
     char **argv;
{
    Cuuid_t u;
    int f;
	
    Cuuid_create(&u);
    printf("Cuuid_create()             -> "); puid(u);

    f = Cuuid_compare(&u, &u);
    printf("Cuuid_compare(u,u): %d\n", f);     /* should be 0 */
    f = Cuuid_compare(&u, &NameSpace_DNS);
    printf("Cuuid_compare(u, NameSpace_DNS): %d\n", f); /* s.b. 1 */
    f = Cuuid_compare(&NameSpace_DNS, &u);
    printf("Cuuid_compare(NameSpace_DNS, u): %d\n", f); /* s.b. -1 */

    Cuuid_create_from_name(&u, NameSpace_DNS, "www.widgets.com");
    printf("Cuuid_create_from_name() -> "); puid(u);
	exit(0);
}

void puid(u)
	Cuuid_t u;
{
    int i;
	
    printf("%08.8x-%04.4x-%04.4x-%02.2x%02.2x-", u.time_low, u.time_mid,
		   u.time_hi_and_version, u.clock_seq_hi_and_reserved,
		   u.clock_seq_low);
    for (i = 0; i < 6; i++)
        printf("%02.2x", u.node[i]);
    printf("\n");
}
