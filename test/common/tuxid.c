#define _POSIX_
#include <stdio.h>
#include "grp.h"
#include "pwd.h"
char *cuserid();
main()
{
	struct group *gr;
	char logname[L_cuserid];
	int n;
	struct passwd *pw;
	printf ("cuserid(NULL)=%s\n", cuserid(NULL));
	printf ("cuserid(...)=%s\n", cuserid(logname));
	printf ("getuid()=%d\n", getuid());
	printf ("getgid()=%d\n", getgid());
	if ((pw = getpwnam ("baud")) == NULL)
		printf ("getpwnam(baud) failed\n");
	else
		printf ("getpwnam(baud) -> pw_name=%s pw_passwd=%s pw_uid=%d pw_gid=%d pw_gecos=%s pw_dir=%s pw_shell=%s\n",
			pw->pw_name, pw->pw_passwd, pw->pw_uid, pw->pw_gid,
			pw->pw_gecos, pw->pw_dir, pw->pw_shell);
	if ((pw = getpwuid (268)) == NULL)
		printf ("getpwuid(268) failed\n");
	else
		printf ("getpwuid(268) -> pw_name=%s pw_passwd=%s pw_uid=%d pw_gid=%d pw_gecos=%s pw_dir=%s pw_shell=%s\n",
			pw->pw_name, pw->pw_passwd, pw->pw_uid, pw->pw_gid,
			pw->pw_gecos, pw->pw_dir, pw->pw_shell);
	if ((gr = getgrgid (1016)) == NULL)
		printf ("getgrgid(1016) failed\n");
	else {
		printf ("getgrgid(1016) -> gr_name=%s gr_passwd=%s gr_gid=%d",
			gr->gr_name, gr->gr_passwd, gr->gr_gid);
		n = 0;
		while (gr->gr_mem[n])
			printf (" gr_mem[%d]=%s", n, gr->gr_mem[n++]);
		printf ("\n");
	}
	if ((gr = getgrgid (1024)) == NULL)
		printf ("getgrgid(1024) failed\n");
	else {
		printf ("getgrgid(1024) -> gr_name=%s gr_passwd=%s gr_gid=%d",
			gr->gr_name, gr->gr_passwd, gr->gr_gid);
		n = 0;
		while (gr->gr_mem[n])
			printf (" gr_mem[%d]=%s", n, gr->gr_mem[n++]);
		printf ("\n");
	}
	if ((gr = getgrgid (1028)) == NULL)
		printf ("getgrgid(1028) failed\n");
	else {
		printf ("getgrgid(1028) -> gr_name=%s gr_passwd=%s gr_gid=%d",
			gr->gr_name, gr->gr_passwd, gr->gr_gid);
		n = 0;
		while (gr->gr_mem[n])
			printf (" gr_mem[%d]=%s", n, gr->gr_mem[n++]);
		printf ("\n");
	}
	if ((gr = getgrnam ("c3")) == NULL)
		printf ("getgrnam(c3) failed\n");
	else {
		printf ("getgrnam(c3) -> gr_name=%s gr_passwd=%s gr_gid=%d",
			gr->gr_name, gr->gr_passwd, gr->gr_gid);
		n = 0;
		while (gr->gr_mem[n])
			printf (" gr_mem[%d]=%s", n, gr->gr_mem[n++]);
		printf ("\n");
	}
	exit (0);
}
