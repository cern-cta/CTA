
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <net.h>
#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>
#include <Ctape_constants.h>
#include <rtcp_api.h>

int main(int argc, char *argv[]) {
    int jid, VolReqID, client_port;
    char *client_host = NULL;
    vdqmDrvReq


