
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/*
 * create_socket
 *
 * Opens a listening TCP socket.
 *
 */
int nw_create_socket(port)
    u_short port;
{
    struct sockaddr_in saddr;
    int s;
    int on = 1;
     
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = INADDR_ANY;

    if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("creating socket");
        return -1;
    }
    /* Let the socket be reused right away */
    (void) setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
    if (bind(s, (struct sockaddr *) &saddr, sizeof(saddr)) < 0) {
        perror("binding socket");
        (void) close(s);
        return -1;
    }

    {
      int yes = 1;
      if (setsockopt(s,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
	perror("Setting TCP_NODELAY");
	return -1;
      }
    }



    if (listen(s, 5) < 0) {
        perror("listening on socket");
        (void) close(s);
        return -1;
    }
    return s;
}

/*
 * Reads n bytes from a file descriptop
 */
ssize_t nw_readn(int fd, void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nread;
    char *ptr;

    ptr=vptr;
    nleft = n;

    while(nleft > 0) {
        if ((nread=read(fd, ptr, nleft))<0) {
            if (errno==EINTR)
                nread=0;
            else
                return -1;
        }  else if (nread == 0)
            break;  /* EOF */
        
        nleft -=nread;
        ptr += nread;
    }
    return n - nleft;
}

/*
 * Reads n bytes from a file descriptop
 */
ssize_t nw_writen(int fd, const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    ptr=vptr;
    nleft = n;

    while(nleft > 0) {
        if ((nwritten=write(fd, ptr, nleft))<0) {
            if (errno==EINTR) {
                nwritten=0;
            } else {
                return -1;
            }
        }
        nleft -=nwritten;
        ptr += nwritten;
    }
    return n - nleft;
}


ssize_t
nw_readline(int fd, void *vptr, size_t maxlen) {

    ssize_t n, rc;
    char c, *ptr;

    ptr=vptr;
    for(n=1; n<maxlen; n++) {
      again:
        if ((rc=read(fd, &c, 1)) == 1) {
            *ptr++=c;
            if (c=='\n')
                break;
        } else if (rc == 0) {
            if (n == 1)
                return 0; /* EOF, no data read */
            else
                break; /* EOF, some data read */
        } else {
            if (errno == EINTR)
                goto again;
            return -1;

        }
    }

    *ptr = 0; /* To terminate with NULL */
    return n;
}


int nw_connect_to_server(host, port)
     char *host;
     u_short port;
{
     struct sockaddr_in saddr;
     struct hostent *hp;
     int s;
     
     if ((hp = gethostbyname(host)) == NULL) {
	  fprintf(stderr, "Unknown host: %s\n", host);
	  return -1;
     }
     
     saddr.sin_family = hp->h_addrtype;
     memcpy((char *)&saddr.sin_addr, hp->h_addr, sizeof(saddr.sin_addr));
     saddr.sin_port = htons(port);

     if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	  perror("creating socket");
	  return -1;
     }

     {
       int yes = 1;
       if (setsockopt(s,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
	 perror("Setting TCP_NODELAY");
	  return -1;
       }
     }

     if (connect(s, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
	  perror("connecting to server");
	  (void) close(s);
	  return -1;
     }
     return s;
}



