/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>
#include <netinet/in.h>

#include "Cgrp.h"
#include "Cthread_api.h"
#include "Cpwd.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_server.h"

extern char localhost[CA_MAXHOSTNAMELEN+1];

void get_client_actual_id (struct vmgr_srv_thread_info *thip)
{
  struct passwd *pw;

#ifdef VMGRCSEC
  if (thip->Csec_service_type < 0) {
    thip->reqinfo.uid = thip->Csec_uid;
    thip->reqinfo.gid = thip->Csec_gid;
  }
#endif
  if ((pw = Cgetpwuid(thip->reqinfo.uid)) == NULL) {
    thip->reqinfo.username = "UNKNOWN";
  } else {
    thip->reqinfo.username = pw->pw_name;
  }
}

/*  vmgr_srv_deletedenmap - delete a triplet model/media_letter/density */

int vmgr_srv_deletedenmap(char *req_data,
                          struct vmgr_srv_thread_info *thip,
                          struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  char density[CA_MAXDENLEN+1];
  char *func = "deletedenmap";
  char media_letter[CA_MAXMLLEN+1];
  char model[CA_MAXMODELLEN+1];
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, density, CA_MAXDENLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLeter=\"%s\" Density=\"%s\"",
           model, media_letter, density);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, model, media_letter,
				      density, &denmap_entry, 1, &rec_addr))
    RETURN (serrno);
  if (vmgr_delete_denmap_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deletedgnmap - delete a triplet dgn/model/library */

int vmgr_srv_deletedgnmap(char *req_data,
                          struct vmgr_srv_thread_info *thip,
                          struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_dgnmap dgnmap_entry;
  char *func = "deletedgnmap";
  char library[CA_MAXTAPELIBLEN+1];
  char model[CA_MAXMODELLEN+1];
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" Library=\"%s\"", model, library);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_dgnmap_entry (&thip->dbfd, model, library, &dgnmap_entry,
			     1, &rec_addr))
    RETURN (serrno);
  if (vmgr_delete_dgnmap_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deletelibrary - delete a tape library definition */

int vmgr_srv_deletelibrary(char *req_data,
                           struct vmgr_srv_thread_info *thip,
                           struct vmgr_srv_request_info *reqinfo)
{
  char *func = "deletelibrary";
  char library[CA_MAXTAPELIBLEN+1];
  struct vmgr_tape_library library_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Library=\"%s\"", library);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_library_entry (&thip->dbfd, library,
			      &library_entry, 1, &rec_addr))
    RETURN (serrno);
  if (library_entry.capacity != library_entry.nb_free_slots) {
    sendrep (thip->s, MSG_ERR, "Cannot remove a library which has tapes associated to it\n");
    RETURN (EEXIST);
  }
  if (vmgr_delete_library_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deletemodel - delete a cartridge model */

int vmgr_srv_deletemodel(char *req_data,
                         struct vmgr_srv_thread_info *thip,
                         struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_media cartridge;
  char *func = "deletemodel";
  char media_letter[CA_MAXMLLEN+1];
  char model[CA_MAXMODELLEN+1];
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLetter=\"%s\"",
           model, media_letter);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 1, &rec_addr))
    RETURN (serrno);
  if (*media_letter && strcmp (media_letter, " ") &&
      strcmp (media_letter, cartridge.m_media_letter))
    RETURN (EINVAL);
  if (vmgr_delete_model_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deletepool - delete a tape pool definition */

int vmgr_srv_deletepool(char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  char *func = "deletepool";
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char pool_name[CA_MAXPOOLNAMELEN+1];
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "PoolName=\"%s\"", pool_name);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, pool_name, &pool_entry,
				    1, &rec_addr))
    RETURN (serrno);
  if (pool_entry.capacity_byte_u64)
    RETURN (EEXIST);
  if (vmgr_delete_pool_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deletetape - delete a tape volume */

int vmgr_srv_deletetape(char *req_data,
			struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  char *func = "deletetape";
  int i;
  struct vmgr_tape_library library_entry;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  vmgr_dbrec_addr pool_rec_addr;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  vmgr_dbrec_addr rec_addrl;
  vmgr_dbrec_addr rec_addrs;
  vmgr_dbrec_addr rec_addrt;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_tag tag_entry;
  struct vmgr_tape_info_byte_u64 tape;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s", vid);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 1, &rec_addr))
    RETURN (serrno);

  /* Delete associated tag if present */
  if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 1, &rec_addrt) == 0) {
    if (vmgr_delete_tag_entry (&thip->dbfd, &rec_addrt))
      RETURN (serrno);
  } else if (serrno != ENOENT)
    RETURN (serrno);

  /* Get native capacity */
  if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, tape.model,
				      tape.media_letter, tape.density,
				      &denmap_entry, 0, NULL))
    RETURN (serrno);

  /* Delete all sides */
  for (i = 0; i < tape.nbsides; i++) {
    if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, i, &side_entry,
					  1, &rec_addrs))
      RETURN (serrno);
    if (side_entry.nbfiles)
      RETURN (EEXIST);
    if (i == 0 && vmgr_get_pool_entry_byte_u64 (&thip->dbfd,
						side_entry.poolname,
						&pool_entry, 1, &pool_rec_addr))
      RETURN (serrno);
    pool_entry.capacity_byte_u64 -= denmap_entry.native_capacity_byte_u64;
    if (side_entry.status == 0)
      pool_entry.tot_free_space_byte_u64 -=
        side_entry.estimated_free_space_byte_u64;
    if (vmgr_delete_side_entry (&thip->dbfd, &rec_addrs))
      RETURN (serrno);
  }
  if (vmgr_delete_tape_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
			      1, &rec_addrl))
    RETURN (serrno);
  library_entry.nb_free_slots++;
  if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl, &library_entry))
    RETURN (serrno);
  if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &pool_rec_addr,
				       &pool_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_deltag - delete a tag associated with a tape volume */

int vmgr_srv_deltag(char *req_data,
                    struct vmgr_srv_thread_info *thip,
                    struct vmgr_srv_request_info *reqinfo)
{
  char *func = "deltag";
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_tag tag_entry;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s", vid);

  /* Get pool name */
  if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, 0, &side_entry,
					0, NULL))
    RETURN (serrno);

  /* Get pool entry */
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
				    &pool_entry, 0, NULL))
    RETURN (serrno);

  /* Check if the user is authorized to delete the tag on this volume */
  if (((pool_entry.uid && pool_entry.uid != reqinfo->uid) ||
       (pool_entry.gid && pool_entry.gid != reqinfo->gid)) &&
      Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_TAPE_OPERATOR))
    RETURN (EACCES);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 1, &rec_addr))
    RETURN (serrno);
  if (vmgr_delete_tag_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_enterdenmap - enter a new quadruplet model/media_letter/density/capacity */

int vmgr_srv_enterdenmap(const int magic,
			 char *const req_data,
			 struct vmgr_srv_thread_info *const thip,
                         struct vmgr_srv_request_info *reqinfo) {
  int native_capacity_mebibyte_int = 0;
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  char *func = "enterdenmap";
  struct vmgr_tape_media model_entry;
  char *rbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &denmap_entry, 0, sizeof(denmap_entry));
  if (unmarshall_STRINGN (rbp, denmap_entry.md_model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, denmap_entry.md_media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, denmap_entry.md_density, CA_MAXDENLEN+1) < 0)
    RETURN (EINVAL);
  if (magic < VMGR_MAGIC2)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, native_capacity_mebibyte_int);
  denmap_entry.native_capacity_byte_u64 =
    (u_signed64)native_capacity_mebibyte_int * ONE_MB;

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "Model=\"%s\" MediaLetter=\"%s\" Density=\"%s\" NativeCapacity=%llu",
           denmap_entry.md_model, denmap_entry.md_media_letter,
           denmap_entry.md_density, denmap_entry.native_capacity_byte_u64);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Check if model exists */
  if (vmgr_get_model_entry (&thip->dbfd, denmap_entry.md_model,
			    &model_entry, 0, NULL))
    RETURN (serrno);
  if (*denmap_entry.md_media_letter &&
      strcmp (denmap_entry.md_media_letter, " ") &&
      strcmp (denmap_entry.md_media_letter, model_entry.m_media_letter))
    RETURN (EINVAL);
  strcpy (denmap_entry.md_media_letter, model_entry.m_media_letter);
  if (native_capacity_mebibyte_int <= 0)
    RETURN (EINVAL);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_denmap_entry_byte_u64 (&thip->dbfd, &denmap_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_enterdenmap - enter a new quadruplet model/media_letter/density/capacity */

int vmgr_srv_enterdenmap_byte_u64(const int magic,
				  char *const req_data,
				  struct vmgr_srv_thread_info *const thip,
                                  struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  char *func = "enterdenmap";
  struct vmgr_tape_media model_entry;
  char *rbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  /* Only VMGR_MAGIC2 is supported */
  if (magic != VMGR_MAGIC2)
    RETURN (EINVAL);

  memset ((char *) &denmap_entry, 0, sizeof(denmap_entry));
  if (unmarshall_STRINGN (rbp, denmap_entry.md_model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, denmap_entry.md_media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, denmap_entry.md_density, CA_MAXDENLEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_HYPER (rbp, denmap_entry.native_capacity_byte_u64);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLetter=\"%s\" Density=\"%s\"",
           denmap_entry.md_model, denmap_entry.md_media_letter,
           denmap_entry.md_density);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Check if model exists */
  if (vmgr_get_model_entry (&thip->dbfd, denmap_entry.md_model,
			    &model_entry, 0, NULL))
    RETURN (serrno);
  if (*denmap_entry.md_media_letter &&
      strcmp (denmap_entry.md_media_letter, " ") &&
      strcmp (denmap_entry.md_media_letter, model_entry.m_media_letter))
    RETURN (EINVAL);
  strcpy (denmap_entry.md_media_letter, model_entry.m_media_letter);
  if (denmap_entry.native_capacity_byte_u64 <= 0)
    RETURN (EINVAL);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_denmap_entry_byte_u64 (&thip->dbfd, &denmap_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_enterdgnmap - enter a new triplet dgn/model/library */

int vmgr_srv_enterdgnmap(char *req_data,
                         struct vmgr_srv_thread_info *thip,
                         struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_dgnmap dgnmap_entry;
  char *func = "enterdgnmap";
  struct vmgr_tape_library library_entry;
  struct vmgr_tape_media model_entry;
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &dgnmap_entry, 0, sizeof(dgnmap_entry));
  if (unmarshall_STRINGN (rbp, dgnmap_entry.dgn, CA_MAXDGNLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, dgnmap_entry.model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, dgnmap_entry.library, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "DGN=\"%s\" Model=\"%s\" Library=\"%s\"",
           dgnmap_entry.dgn, dgnmap_entry.model, dgnmap_entry.library);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Check if model exists */
  if (vmgr_get_model_entry (&thip->dbfd, dgnmap_entry.model, &model_entry,
			    0, NULL))
    RETURN (serrno);

  /* Check if library exists */
  if (vmgr_get_library_entry (&thip->dbfd, dgnmap_entry.library,
			      &library_entry, 0, NULL))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_dgnmap_entry (&thip->dbfd, &dgnmap_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_enterlibrary - enter a new tape library definition */

int vmgr_srv_enterlibrary(char *req_data,
                          struct vmgr_srv_thread_info *thip,
                          struct vmgr_srv_request_info *reqinfo)
{
  char *func = "enterlibrary";
  struct vmgr_tape_library library_entry;
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &library_entry, 0, sizeof(library_entry));
  if (unmarshall_STRINGN (rbp, library_entry.name, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, library_entry.capacity);
  unmarshall_LONG (rbp, library_entry.status);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Library=\"%s\" Capacity=%d Status=%d",
           library_entry.name, library_entry.capacity, library_entry.status);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  if (library_entry.capacity <= 0)
    RETURN (EINVAL);
  library_entry.nb_free_slots = library_entry.capacity;

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_library_entry (&thip->dbfd, &library_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_entermodel - enter a new cartridge model */

int vmgr_srv_entermodel(int magic,
                        char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_media cartridge;
  char *func = "entermodel";
  int native_capacity;
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &cartridge, 0, sizeof(cartridge));
  if (unmarshall_STRINGN (rbp, cartridge.m_model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, cartridge.m_media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);
  if (magic < VMGR_MAGIC2)
    unmarshall_LONG (rbp, native_capacity);  /* ignore */
  unmarshall_LONG (rbp, cartridge.media_cost);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLetter=\"%s\" MediaCost=%d",
           cartridge.m_model, cartridge.m_media_letter, cartridge.media_cost);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_model_entry (&thip->dbfd, &cartridge))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_enterpool - define a new tape pool */

int vmgr_srv_enterpool(char *req_data,
                       struct vmgr_srv_thread_info *thip,
                       struct vmgr_srv_request_info *reqinfo)
{
  char *func = "enterpool";
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &pool_entry, 0, sizeof(pool_entry));
  if (unmarshall_STRINGN (rbp, pool_entry.name, CA_MAXPOOLNAMELEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, pool_entry.uid);
  unmarshall_LONG (rbp, pool_entry.gid);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "PoolName=\"%s\" PoolUid=%d PoolGid=%d",
           pool_entry.name, pool_entry.uid, pool_entry.gid);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  if (vmgr_insert_pool_entry_byte_u64 (&thip->dbfd, &pool_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_entertape - enter a new tape volume */

int vmgr_srv_entertape(char *req_data,
                       struct vmgr_srv_thread_info *thip,
                       struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_media cartridge;
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  struct vmgr_tape_dgnmap dgnmap_entry;
  char *func = "entertape";
  int i;
  struct vmgr_tape_library library_entry;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  vmgr_dbrec_addr rec_addrl;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_info_byte_u64 tape;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &tape, 0, sizeof(tape));
  memset ((char *) &side_entry, 0, sizeof(side_entry));
  if (unmarshall_STRINGN (rbp, tape.vid, CA_MAXVIDLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.vsn, CA_MAXVSNLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.library, CA_MAXTAPELIBLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.density, CA_MAXDENLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.lbltype, CA_MAXLBLTYPLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.model, CA_MAXMODELLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.media_letter, CA_MAXMLLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.manufacturer, CA_MAXMANUFLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tape.sn, CA_MAXSNLEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, tape.nbsides);
  if (unmarshall_STRINGN (rbp, side_entry.poolname, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, side_entry.status);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s PoolName=\"%s\" SideStatus=%d",
           tape.vid, side_entry.poolname, side_entry.status);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_TAPE_OPERATOR))
    RETURN (serrno);

  if (! *tape.vsn)
    strcpy (tape.vsn, tape.vid);
  if (*tape.lbltype) {
    if (strcmp (tape.lbltype, "al") &&
        strcmp (tape.lbltype, "aul") &&
        strcmp (tape.lbltype, "nl") &&
        strcmp (tape.lbltype, "sl"))
      RETURN (EINVAL);
  } else
    strcpy (tape.lbltype, "al");
  if (! *side_entry.poolname)
    strcpy (side_entry.poolname, "default");

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Check if pool exists and lock entry */
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
				    &pool_entry, 1, &rec_addr)) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such pool\n");
      RETURN (EINVAL);
    } else
      RETURN (serrno);
  }

  /* Check if model exists */
  memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
  if (vmgr_get_model_entry (&thip->dbfd, tape.model, &cartridge, 0, NULL)) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such model\n");
      RETURN (EINVAL);
    } else
      RETURN (serrno);
  }

  if (*tape.media_letter && strcmp (tape.media_letter, " ") &&
      strcmp (tape.media_letter, cartridge.m_media_letter))
    RETURN (EINVAL);
  strcpy (tape.media_letter, cartridge.m_media_letter);

  /* Check if library exists */
  memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
  if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
			      1, &rec_addrl)) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such library\n");
      RETURN (EINVAL);
    } else
      RETURN (serrno);
  }

  /* Check if the combination library/model exists */
  if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
			     &dgnmap_entry, 0, NULL)) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR,
	       "Combination library/model does not exist\n");
      RETURN (EINVAL);
    } else
      RETURN (serrno);
  }

  /* Check if density is valid for this model and get native capacity */
  memset((void *) &denmap_entry, 0, sizeof(denmap_entry));
  if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, tape.model,
                                      tape.media_letter, tape.density,
                                      &denmap_entry, 0, NULL)) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR,
	       "Unsupported density for this model\n");
      RETURN (EINVAL);
    } else
      RETURN (serrno);
  }
  side_entry.estimated_free_space_byte_u64 =
    denmap_entry.native_capacity_byte_u64;

  tape.etime = time (0);
  strcpy (tape.rhost, "N/A");
  strcpy (tape.whost, "N/A");
  if (vmgr_insert_tape_entry_byte_u64 (&thip->dbfd, &tape))
    RETURN (serrno);

  strcpy (side_entry.vid, tape.vid);
  for (i = 0; i < tape.nbsides; i++) {
    side_entry.side = i;
    if (vmgr_insert_side_entry_byte_u64 (&thip->dbfd, &side_entry))
      RETURN (serrno);
    pool_entry.capacity_byte_u64 += denmap_entry.native_capacity_byte_u64;
    if (side_entry.status == 0 || side_entry.status == TAPE_FULL) // Tape can be full but having free space
      pool_entry.tot_free_space_byte_u64 +=
        denmap_entry.native_capacity_byte_u64;
  }

  library_entry.nb_free_slots--;
  if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl, &library_entry))
    RETURN (serrno);

  if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &rec_addr,
				       &pool_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_gettag - get the tag associated with a tape volume */

int vmgr_srv_gettag(char *req_data,
                    struct vmgr_srv_thread_info *thip,
                    struct vmgr_srv_request_info *reqinfo)
{
  char *func = "gettag";
  struct vmgr_tape_tag tag_entry;
  char *rbp;
  char repbuf[CA_MAXTAGLEN+1];
  char *sbp;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s", vid);

  if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 0, NULL))
    RETURN (serrno);
  sbp = repbuf;
  marshall_STRING (sbp, tag_entry.text);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_gettape - get a tape volume to store a given amount of data */

int vmgr_srv_gettape(int magic,
                     char *req_data,
                     struct vmgr_srv_thread_info *thip,
                     struct vmgr_srv_request_info *reqinfo)
{
  char condition[512];
  struct vmgr_tape_dgnmap dgnmap_entry;
  int fseq;
  char *func = "gettape";
  int i;
  static int onealloc;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char poolname[CA_MAXPOOLNAMELEN+1];
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  char repbuf[55];
  int save_serrno;
  char *sbp;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_side_byte_u64 side_entry1;
  u_signed64 size;
  struct vmgr_tape_info_byte_u64 tape;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_HYPER (rbp, size);
  if (unmarshall_STRINGN (rbp, condition, 512) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "PoolName=\"%s\" RequestedSize=%llu",
           poolname, size);

  if (! *poolname)
    strcpy (poolname, "default");
  else {  /* Check if pool exists and permissions are ok */
    if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, poolname, &pool_entry,
				      0, NULL))
      RETURN (serrno);
    if (((pool_entry.uid > 0 && reqinfo->uid != pool_entry.uid) ||
	 (pool_entry.gid > 0 && reqinfo->gid != pool_entry.gid)) &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                    P_ADMIN))
      RETURN (EACCES);
  }
  if (size <= 0)
    RETURN (EINVAL);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);
  if (Cthread_mutex_lock (&onealloc))
    RETURN (serrno);

  /* Get and lock entry */
  memset ((void *) &side_entry, 0, sizeof(side_entry));
  if (vmgr_get_side_by_size_byte_u64 (&thip->dbfd, poolname, size, &side_entry,
				      1, &rec_addr)) {
    if (serrno == ENOENT)
      serrno = ENOSPC;
    save_serrno = serrno;
    (void) Cthread_mutex_unlock (&onealloc);
    RETURN (save_serrno);
  }

  /* Update entry */
  side_entry.status = TAPE_BUSY;

  if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addr, &side_entry)) {
    save_serrno = serrno;
    (void) Cthread_mutex_unlock (&onealloc);
    RETURN (save_serrno);
  }

  /* Get base entry */
  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, side_entry.vid, &tape, 0,
				     NULL)) {
    save_serrno = serrno;
    (void) Cthread_mutex_unlock (&onealloc);
    RETURN (save_serrno);
  }

  /* If the media offers several sides, mark all sides busy if they are not
   * FULL
   */
  for (i = 0; i < tape.nbsides; i++) {
    if (i == side_entry.side) continue;
    if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, side_entry.vid, i,
					  &side_entry1, 1, &rec_addr)) {
      save_serrno = serrno;
      (void) Cthread_mutex_unlock (&onealloc);
      RETURN (save_serrno);
    }
    if (side_entry1.status == 0)
      side_entry1.status = TAPE_BUSY;
    if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addr,
					 &side_entry1)) {
      save_serrno = serrno;
      (void) Cthread_mutex_unlock (&onealloc);
      RETURN (save_serrno);
    }
  }

  /* Get dgn */
  if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
			     &dgnmap_entry, 0, NULL)) {
    save_serrno = serrno;
    (void) Cthread_mutex_unlock (&onealloc);
    RETURN (save_serrno);
  }

  fseq = side_entry.nbfiles + 1;

  /* Ammend log message */
  sprintf (reqinfo->logbuf,
           "PoolName=\"%s\" RequestedSize=%llu TPVID=%s Side=%d Fseq=%d",
           poolname, size, tape.vid, side_entry.side, fseq);

  sbp = repbuf;
  marshall_STRING (sbp, tape.vid);
  marshall_STRING (sbp, tape.vsn);
  marshall_STRING (sbp, dgnmap_entry.dgn);
  marshall_STRING (sbp, tape.density);
  marshall_STRING (sbp, tape.lbltype);
  marshall_STRING (sbp, tape.model);
  if (magic >= VMGR_MAGIC2) {
    marshall_WORD (sbp, side_entry.side);
  }
  marshall_LONG (sbp, fseq);
  marshall_HYPER (sbp, side_entry.estimated_free_space_byte_u64);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  (void) Cthread_mutex_unlock (&onealloc);
  RETURN (0);
}

/*      vmgr_srv_listdenmap - list triplets model/media_letter/density */

int vmgr_srv_listdenmap(const int magic,
			char *const req_data,
			struct vmgr_srv_thread_info *const thip,
                        struct vmgr_srv_request_info *reqinfo,
			const int endlist) {
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  int eol = 0;  /* end of list flag */
  char *func = "listdenmap";
  int listentsz = 0;  /* size of client machine vmgr_tape_denmap structure */
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_denmap_entry_byte_u64 (&thip->dbfd, bol, &denmap_entry,
					       endlist)) == 0) {
    marshall_STRING (sbp, denmap_entry.md_model);
    marshall_STRING (sbp, denmap_entry.md_media_letter);
    marshall_STRING (sbp, denmap_entry.md_density);
    if (magic >= VMGR_MAGIC2) {
      int native_capacity_mebibyte_int = 0;

      /* Give the native capacity in mebibytes a ceiling of the  */
      /* largest posible positive 32-bit signed integer which is */
      /* 2^32-1 = 2147483647                                     */
      if(denmap_entry.native_capacity_byte_u64 / ONE_MB > 2147483647) {
        native_capacity_mebibyte_int = 2147483647;
      } else {
        native_capacity_mebibyte_int = denmap_entry.native_capacity_byte_u64 /
          ONE_MB;
      }
      marshall_LONG (sbp, native_capacity_mebibyte_int);
    }
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listdenmap - list triplets model/media_letter/density */

int vmgr_srv_listdenmap_byte_u64(const int magic,
				 char *const req_data,
				 struct vmgr_srv_thread_info *const thip,
                                 struct vmgr_srv_request_info *reqinfo,
				 const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  int eol = 0;  /* end of list flag */
  char *func = "listdenmap";
  /* listentsz = size of client machine vmgr_tape_denmap_byte_u64 structure */
  int listentsz = 0;
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  /* Only VMGR_MAGIC2 is supported */
  if (magic != VMGR_MAGIC2)
    RETURN (EINVAL);

  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_denmap_entry_byte_u64 (&thip->dbfd, bol,
					       &denmap_entry, endlist)) == 0) {
    marshall_STRING (sbp, denmap_entry.md_model);
    marshall_STRING (sbp, denmap_entry.md_media_letter);
    marshall_STRING (sbp, denmap_entry.md_density);
    marshall_HYPER (sbp, denmap_entry.native_capacity_byte_u64);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listdgnmap - list triplets model/media_letter/density */

int vmgr_srv_listdgnmap(char *const req_data,
                        struct vmgr_srv_thread_info *const thip,
                        struct vmgr_srv_request_info *reqinfo,
                        const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  struct vmgr_tape_dgnmap dgnmap_entry;
  int eol = 0;  /* end of list flag */
  char *func = "listdgnmap";
  int listentsz = 0;  /* size of client machine vmgr_tape_dgnmap structure */
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_dgnmap_entry (&thip->dbfd, bol, &dgnmap_entry,
                                      endlist)) == 0) {
    marshall_STRING (sbp, dgnmap_entry.dgn);
    marshall_STRING (sbp, dgnmap_entry.model);
    marshall_STRING (sbp, dgnmap_entry.library);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listlibrary - list tape library entries */

int vmgr_srv_listlibrary(char *const req_data,
                         struct vmgr_srv_thread_info *const thip,
                         struct vmgr_srv_request_info *reqinfo,
                         const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  int eol = 0;  /* end of list flag */
  char *func = "listlibrary";
  struct vmgr_tape_library library_entry;
  int listentsz = 0;  /* size of client machine vmgr_tape_library structure */
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_library_entry (&thip->dbfd, bol, &library_entry,
                                       endlist)) == 0) {
    marshall_STRING (sbp, library_entry.name);
    marshall_LONG (sbp, library_entry.capacity);
    marshall_LONG (sbp, library_entry.nb_free_slots);
    marshall_LONG (sbp, library_entry.status);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listmodel - list cartridge model entries */

int vmgr_srv_listmodel(const int magic,
                       char *const req_data,
                       struct vmgr_srv_thread_info *const thip,
                       struct vmgr_srv_request_info *reqinfo,
                       const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  struct vmgr_tape_media cartridge;
  int eol = 0;  /* end of list flag */
  char *func = "listmodel";
  int listentsz = 0;  /* size of client machine vmgr_tape_media structure */
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_model_entry (&thip->dbfd, bol, &cartridge,
                                     endlist)) == 0) {
    marshall_STRING (sbp, cartridge.m_model);
    marshall_STRING (sbp, cartridge.m_media_letter);
    if (magic < VMGR_MAGIC2)
      marshall_LONG (sbp, 0);  /* Dummy */
    marshall_LONG (sbp, cartridge.media_cost);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listpool - list tape pool entries */

int vmgr_srv_listpool(char *const req_data,
                      struct vmgr_srv_thread_info *const thip,
                      struct vmgr_srv_request_info *reqinfo,
                      const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  int eol = 0;  /* end of list flag */
  char *func = "listpool";
  int listentsz = 0;  /* size of client machine vmgr_tape_pool structure */
  int maxnbentries = 0;  /* maximum number of entries/call */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p = NULL;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp = NULL;
  char *sbp = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / listentsz;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_pool_entry_byte_u64 (&thip->dbfd, bol, &pool_entry,
                                             endlist)) == 0) {
    marshall_STRING (sbp, pool_entry.name);
    marshall_LONG (sbp, pool_entry.uid);
    marshall_LONG (sbp, pool_entry.gid);
    marshall_HYPER (sbp, pool_entry.capacity_byte_u64);
    marshall_HYPER (sbp, pool_entry.tot_free_space_byte_u64);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listtape - list tape volume entries */

int vmgr_srv_listtape(const int magic,
		      char *const req_data,
		      struct vmgr_srv_thread_info *const thip,
                      struct vmgr_srv_request_info *reqinfo,
		      struct vmgr_tape_info_byte_u64 *const tape,
		      const int endlist) {
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  struct vmgr_tape_dgnmap dgnmap_entry;
  int eol = 0;  /* end of list flag */
  char *func = "listtape";
  int listentsz = 0;  /* size of client machine vmgr_tape_info structure */
  int maxnbentries = 0; /* maximum number of entries per output buffer */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4]; /* nbentries(2 bytes) + eol(2 bytes) = 4 bytes */
  char *p = NULL;
  char pool_name[CA_MAXPOOLNAMELEN+1];
  struct vmgr_tape_side_byte_u64 side_entry;
  char *rbp = NULL;
  char *sbp = NULL;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  if (magic >= VMGR_MAGIC2) {
    if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN + 1) < 0)
      RETURN (EINVAL);
  } else
    vid[0] = '\0';
  if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / ((MARSHALLED_TAPE_ENTRYSZ > listentsz) ?
			      MARSHALLED_TAPE_ENTRYSZ : listentsz);
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_side_entry_byte_u64 (&thip->dbfd, bol, vid, pool_name,
					     &side_entry, endlist)) == 0) {
    if (bol || strcmp (tape->vid, side_entry.vid))
      if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, side_entry.vid,
					 tape, 0, NULL))
        RETURN (serrno);
    marshall_STRING (sbp, tape->vid);
    marshall_STRING (sbp, tape->vsn);
    if (magic < VMGR_MAGIC2) {
      if (vmgr_get_dgnmap_entry (&thip->dbfd, tape->model,
				 tape->library, &dgnmap_entry, 0, NULL))
        RETURN (serrno);
      marshall_STRING (sbp, dgnmap_entry.dgn);
    } else
      marshall_STRING (sbp, tape->library);
    marshall_STRING (sbp, tape->density);
    if (magic < VMGR_MAGIC2)
      tape->lbltype[2] = '\0';
    marshall_STRING (sbp, tape->lbltype);
    marshall_STRING (sbp, tape->model);
    marshall_STRING (sbp, tape->media_letter);
    marshall_STRING (sbp, tape->manufacturer);
    marshall_STRING (sbp, tape->sn);
    if (magic >= VMGR_MAGIC2) {
      marshall_WORD (sbp, tape->nbsides);
      marshall_TIME_T (sbp, tape->etime);
      marshall_WORD (sbp, side_entry.side);
    }
    marshall_STRING (sbp, side_entry.poolname);
    {
      /* Give the estimated free-space in kibibytes a ceiling of the */
      /* largest posible positive 32-bit signed integer which is     */
      /* 2^32-1 = 2147483647                                         */
      int estimated_free_space_kibibyte_int = 0;
      if(side_entry.estimated_free_space_byte_u64 / ONE_KB > 2147483647) {
        estimated_free_space_kibibyte_int = 2147483647;
      } else {
        estimated_free_space_kibibyte_int =
          side_entry.estimated_free_space_byte_u64 / ONE_KB;
      }
      marshall_LONG (sbp, estimated_free_space_kibibyte_int);
    }
    marshall_LONG (sbp, side_entry.nbfiles);
    marshall_LONG (sbp, tape->rcount);
    marshall_LONG (sbp, tape->wcount);
    if (magic >= VMGR_MAGIC2) {
      marshall_STRING (sbp, tape->rhost);
      marshall_STRING (sbp, tape->whost);
      marshall_LONG (sbp, tape->rjid);
      marshall_LONG (sbp, tape->wjid);
    }
    marshall_TIME_T (sbp, tape->rtime);
    marshall_TIME_T (sbp, tape->wtime);
    marshall_LONG (sbp, side_entry.status);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*      vmgr_srv_listtape - list tape volume entries */

int vmgr_srv_listtape_byte_u64(const int magic,
			       char *const req_data,
			       struct vmgr_srv_thread_info *const thip,
                               struct vmgr_srv_request_info *reqinfo,
			       struct vmgr_tape_info_byte_u64 *const tape,
			       const int endlist)
{
  int bol = 0;  /* beginning of list flag */
  int c = 0;
  int eol = 0;  /* end of list flag */
  char *func = "listtape";
  int listentsz = 0;  /* size of client machine vmgr_tape_info structure */
  int maxnbentries = 0; /* maximum number of entries per output buffer */
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4]; /* nbentries(2 bytes) + eol(2 bytes) = 4 bytes */
  char *p = NULL;
  char pool_name[CA_MAXPOOLNAMELEN+1];
  struct vmgr_tape_side_byte_u64 side_entry;
  char *rbp = NULL;
  char *sbp = NULL;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  /* Only VMGR_MAGIC2 is supported */
  if (magic != VMGR_MAGIC2)
    RETURN (EINVAL);

  unmarshall_WORD (rbp, listentsz);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bol);

  /* Return as many entries as possible to the client */
  maxnbentries = LISTBUFSZ / ((MARSHALLED_TAPE_BYTE_U64_ENTRYSZ > listentsz) ?
			      MARSHALLED_TAPE_BYTE_U64_ENTRYSZ : listentsz);
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);    /* Will be updated */

  while (nbentries < maxnbentries &&
	 (c = vmgr_list_side_entry_byte_u64 (&thip->dbfd, bol, vid, pool_name,
					     &side_entry, endlist)) == 0) {
    if (bol || strcmp (tape->vid, side_entry.vid))
      if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, side_entry.vid,
					 tape, 0, NULL))
        RETURN (serrno);
    marshall_STRING (sbp, tape->vid);
    marshall_STRING (sbp, tape->vsn);
    marshall_STRING (sbp, tape->library);
    marshall_STRING (sbp, tape->density);
    marshall_STRING (sbp, tape->lbltype);
    marshall_STRING (sbp, tape->model);
    marshall_STRING (sbp, tape->media_letter);
    marshall_STRING (sbp, tape->manufacturer);
    marshall_STRING (sbp, tape->sn);
    marshall_WORD (sbp, tape->nbsides);
    marshall_TIME_T (sbp, tape->etime);
    marshall_WORD (sbp, side_entry.side);
    marshall_STRING (sbp, side_entry.poolname);
    marshall_HYPER (sbp, side_entry.estimated_free_space_byte_u64);
    marshall_LONG (sbp, side_entry.nbfiles);
    marshall_LONG (sbp, tape->rcount);
    marshall_LONG (sbp, tape->wcount);
    marshall_STRING (sbp, tape->rhost);
    marshall_STRING (sbp, tape->whost);
    marshall_LONG (sbp, tape->rjid);
    marshall_LONG (sbp, tape->wjid);
    marshall_TIME_T (sbp, tape->rtime);
    marshall_TIME_T (sbp, tape->wtime);
    marshall_LONG (sbp, side_entry.status);
    nbentries++;
    bol = 0;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);    /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

/*  vmgr_srv_modifylibrary - modify an existing tape library */

int vmgr_srv_modifylibrary(char *req_data,
                           struct vmgr_srv_thread_info *thip,
                           struct vmgr_srv_request_info *reqinfo)
{
  int capacity;
  char *func = "modifylibrary";
  char library[CA_MAXTAPELIBLEN+1];
  struct vmgr_tape_library library_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  int status;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, capacity);
  unmarshall_LONG (rbp, status);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Library=\"%s\" Capacity=%d Status=%d",
           library, capacity, status);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entry */
  memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
  if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
			      1, &rec_addr))
    RETURN (serrno);

  /* Update entry */
  if (capacity > 0) {
    library_entry.nb_free_slots += capacity - library_entry.capacity;
    library_entry.capacity = capacity;
  }
  if (status >= 0)
    library_entry.status = status;

  if (vmgr_update_library_entry (&thip->dbfd, &rec_addr, &library_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_modifymodel - modify an existing cartridge model */

int vmgr_srv_modifymodel(int magic,
                         char *req_data,
                         struct vmgr_srv_thread_info *thip,
                         struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_media cartridge;
  char *func = "modifymodel";
  int media_cost;
  char media_letter[CA_MAXMLLEN+1];
  char model[CA_MAXMODELLEN+1];
  int native_capacity;
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);
  if (magic < VMGR_MAGIC2)
    unmarshall_LONG (rbp, native_capacity);  /* ignore */
  unmarshall_LONG (rbp, media_cost);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLetter=\"%s\" MediaCost=%d",
           model, media_letter, media_cost);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entry */
  memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
  if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 1, &rec_addr))
    RETURN (serrno);
  if (*media_letter && strcmp (media_letter, " ") &&
      strcmp (media_letter, cartridge.m_media_letter))
    RETURN (EINVAL);

  /* Update entry */
  if (media_cost)
    cartridge.media_cost = media_cost;

  if (vmgr_update_model_entry (&thip->dbfd, &rec_addr, &cartridge))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_modifypool - modify an existing tape pool definition */

int vmgr_srv_modifypool(char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  char *func = "modifypool";
  struct vmgr_tape_pool_byte_u64 pool_entry;
  gid_t pool_group;
  char pool_name[CA_MAXPOOLNAMELEN+1];
  uid_t pool_user;
  char *rbp;
  vmgr_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, pool_user);
  unmarshall_LONG (rbp, pool_group);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "PoolName=\"%s\" PoolUid=%d PoolGid=%d",
           pool_name, pool_user, pool_group);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entry */
  memset((void *) &pool_entry, 0, sizeof(pool_entry));
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, pool_name, &pool_entry,
				    1, &rec_addr))
    RETURN (serrno);

  /* Update entry */
  if (pool_user != (uid_t)-1)
    pool_entry.uid = pool_user;
  if (pool_group != (gid_t)-1)
    pool_entry.gid = pool_group;

  if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &rec_addr, &pool_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_modifytape - modify an existing tape volume */

int vmgr_srv_modifytape(char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  int capacity_changed = 0;
  int i;
  int need_update = 0;
  int status;

  char density[CA_MAXDENLEN+1];
  char lbltype[CA_MAXLBLTYPLEN+1];
  char library[CA_MAXTAPELIBLEN+1];
  char *func = "modifytape";
  char manufacturer[CA_MAXMANUFLEN+1];
  char *rbp;
  char sn[CA_MAXSNLEN+1];
  char vid[CA_MAXVIDLEN+1];
  char vsn[CA_MAXVSNLEN+1];
  char poolname[CA_MAXPOOLNAMELEN+1];

  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  struct vmgr_tape_dgnmap dgnmap_entry;
  struct vmgr_tape_library library_entry;
  struct vmgr_tape_pool_byte_u64 newpool_entry;
  struct vmgr_tape_denmap_byte_u64 olddenmap_entry;
  struct vmgr_tape_pool_byte_u64 oldpool_entry;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_info_byte_u64 tape;

  vmgr_dbrec_addr newpool_rec_addr;
  vmgr_dbrec_addr oldpool_rec_addr;
  vmgr_dbrec_addr rec_addr;
  vmgr_dbrec_addr rec_addrl;
  vmgr_dbrec_addr rec_addrs;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, vsn, CA_MAXVSNLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, density, CA_MAXDENLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, lbltype, CA_MAXLBLTYPLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, manufacturer, CA_MAXMANUFLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, sn, CA_MAXSNLEN + 1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, status);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s PoolName=\"%s\" Status=%d",
           vid, poolname, status);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_TAPE_OPERATOR))
    RETURN (serrno);

  if (*lbltype) {
    if (strcmp (lbltype, "al") &&
        strcmp (lbltype, "aul") &&
        strcmp (lbltype, "nl") &&
        strcmp (lbltype, "sl"))
      RETURN (EINVAL);
  }

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock tape entry */
  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 1, &rec_addr))
    RETURN (serrno);

  if (*vsn && strcmp (vsn, tape.vsn)) {
    strcpy (tape.vsn, vsn);
    need_update++;
  }

  /* Check if library exists and update the number of free slots */
  if (*library && strcmp (library, tape.library)) {
    memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
    if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
				1, &rec_addrl)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such library\n");
        RETURN (EINVAL);
      } else
        RETURN (serrno);
    }

    /* Check if the combination library/model exists */
    if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, library,
			       &dgnmap_entry, 0, NULL)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR,
		 "Combination library/model does not exist\n");
        RETURN (EINVAL);
      } else
        RETURN (serrno);
    }

    library_entry.nb_free_slots--;
    if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl,
				   &library_entry))
      RETURN (serrno);

    memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
    if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
				1, &rec_addrl))
      RETURN (serrno);
    library_entry.nb_free_slots++;
    if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl,
				   &library_entry))
      RETURN (serrno);
    strcpy (tape.library, library);
    need_update++;
  }

  /* Get current tape capacity */
  if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, tape.model,
				      tape.media_letter, tape.density,
				      &olddenmap_entry, 0, NULL))
    RETURN (serrno);

  /* Check if density is valid for this model */
  int density_changed = 0;
  if (*density && strcmp (density, tape.density)) {
    memset((void *) &denmap_entry, 0, sizeof(denmap_entry));
    if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, tape.model,
					tape.media_letter, density,
					&denmap_entry, 0, NULL)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR,
		 "Unsupported density for this model\n");
        RETURN (EINVAL);
      } else
        RETURN (serrno);
    }
    strcpy (tape.density, density);
    density_changed = 1;
    if (denmap_entry.native_capacity_byte_u64 !=
	olddenmap_entry.native_capacity_byte_u64)
      capacity_changed = 1;
    need_update++;
  } else
    memcpy (&denmap_entry, &olddenmap_entry, sizeof(denmap_entry));

  int lbltype_changed = 0;
  if (*lbltype && strcmp (lbltype, tape.lbltype)) {
    strcpy (tape.lbltype, lbltype);
    lbltype_changed = 1;
    need_update++;
  }
  if (*manufacturer && strcmp (manufacturer, tape.manufacturer)) {
    strcpy (tape.manufacturer, manufacturer);
    need_update++;
  }
  if (*sn && strcmp (sn, tape.sn)) {
    strcpy (tape.sn, sn);
    need_update++;
  }

  if (need_update)
    if (vmgr_update_tape_entry_byte_u64 (&thip->dbfd, &rec_addr, &tape))
      RETURN (serrno);

  for (i = 0; i < tape.nbsides; i++) {
    need_update = 0;

    /* Get and lock side */
    if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, i, &side_entry,
					  1, &rec_addrs))
      RETURN (serrno);

    /* Return EEXIST error if the user request a density or lable type */
    /* modification on a tape that still contain physical tape files   */
    if ((density_changed || lbltype_changed) &&
        (side_entry.nbfiles || (side_entry.status & TAPE_BUSY)))
      RETURN (EEXIST);

    if (*poolname == 0 ||
        strcmp (poolname, side_entry.poolname) == 0) { /* Same pool */
      if (((side_entry.status & ~TAPE_BUSY) == 0 &&
	   (status & ~TAPE_BUSY) > 0) ||
          ((side_entry.status & ~TAPE_BUSY) > 0 &&
	   (status & ~TAPE_BUSY) == 0) ||
          capacity_changed) {
        if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
					  &oldpool_entry, 1, &oldpool_rec_addr))
          RETURN (serrno);
        if (capacity_changed) {
          oldpool_entry.capacity_byte_u64 +=
	    denmap_entry.native_capacity_byte_u64 -
	    olddenmap_entry.native_capacity_byte_u64;
          if (side_entry.status == 0)
            oldpool_entry.tot_free_space_byte_u64 -=
	      side_entry.estimated_free_space_byte_u64;
          side_entry.estimated_free_space_byte_u64 =
	    denmap_entry.native_capacity_byte_u64;
          need_update++;
          if (status == 0 ||
              (status < 0 && side_entry.status == 0))
            oldpool_entry.tot_free_space_byte_u64 +=
	      side_entry.estimated_free_space_byte_u64;
        } else {  /* Status changed */
          if (status & ~TAPE_BUSY)
            oldpool_entry.tot_free_space_byte_u64 -=
	      side_entry.estimated_free_space_byte_u64;
          else
            oldpool_entry.tot_free_space_byte_u64 +=
	      side_entry.estimated_free_space_byte_u64;
        }
        if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &oldpool_rec_addr,
					     &oldpool_entry))
          RETURN (serrno);
      }
    } else {  /* Move to another pool */
      if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
					&oldpool_entry, 1, &oldpool_rec_addr))
        RETURN (serrno);
      oldpool_entry.capacity_byte_u64 -=
        olddenmap_entry.native_capacity_byte_u64;
      if (side_entry.status == 0)
        oldpool_entry.tot_free_space_byte_u64 -=
          side_entry.estimated_free_space_byte_u64;
      if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd,
					   &oldpool_rec_addr, &oldpool_entry))
        RETURN (serrno);
      if (capacity_changed) {
        side_entry.estimated_free_space_byte_u64 =
          denmap_entry.native_capacity_byte_u64;
        need_update++;
      }
      if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, poolname,
					&newpool_entry, 1, &newpool_rec_addr)) {
        if (serrno == ENOENT) {
          sendrep (thip->s, MSG_ERR, "No such pool\n");
          RETURN (EINVAL);
        } else
          RETURN (serrno);
      }
      newpool_entry.capacity_byte_u64 += denmap_entry.native_capacity_byte_u64;
      if (status == 0 || (status < 0 && side_entry.status == 0))
        newpool_entry.tot_free_space_byte_u64 +=
          side_entry.estimated_free_space_byte_u64;
      if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd,
					   &newpool_rec_addr, &newpool_entry))
        RETURN (serrno);
      strcpy (side_entry.poolname, poolname);
      need_update++;
    }
    if (status >= 0) {
      if (side_entry.estimated_free_space_byte_u64 == 0) {
	status |= TAPE_FULL;
      }
      side_entry.status = status;
      need_update++;
    }

    if (need_update)
      if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addrs,
					   &side_entry))
        RETURN (serrno);
  }
  RETURN (0);
}

/*  vmgr_srv_querypool - query about a tape pool */

int vmgr_srv_querypool(char *req_data,
                       struct vmgr_srv_thread_info *thip,
                       struct vmgr_srv_request_info *reqinfo)
{
  char *func = "querypool";
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char pool_name[CA_MAXPOOLNAMELEN+1];
  char *rbp;
  char repbuf[24];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN + 1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "PoolName=\"%s\"", pool_name);

  memset((void *) &pool_entry, 0, sizeof(pool_entry));
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, pool_name, &pool_entry,
				    0, NULL))
    RETURN (serrno);

  sbp = repbuf;
  marshall_LONG (sbp, pool_entry.uid);
  marshall_LONG (sbp, pool_entry.gid);
  marshall_HYPER (sbp, pool_entry.capacity_byte_u64);
  marshall_HYPER (sbp, pool_entry.tot_free_space_byte_u64);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_querylibrary - query about a tape library */

int vmgr_srv_querylibrary(char *req_data,
                          struct vmgr_srv_thread_info *thip,
                          struct vmgr_srv_request_info *reqinfo)
{
  char *func = "querylibrary";
  char library[CA_MAXTAPELIBLEN+1];
  struct vmgr_tape_library library_entry;
  char *rbp;
  char repbuf[12];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Library=\"%s\"", library);

  memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
  if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
			      0, NULL))
    RETURN (serrno);

  sbp = repbuf;
  marshall_LONG (sbp, library_entry.capacity);
  marshall_LONG (sbp, library_entry.nb_free_slots);
  marshall_LONG (sbp, library_entry.status);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_querymodel - query about a cartridge model */

int vmgr_srv_querymodel(int magic,
                        char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_media cartridge;
  char *func = "querymodel";
  char media_letter[CA_MAXMLLEN+1];
  char model[CA_MAXMODELLEN+1];
  char *rbp;
  char repbuf[11];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Model=\"%s\" MediaLetter=\"%s\"",
           model, media_letter);

  memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
  if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 0, NULL))
    RETURN (serrno);
  if (*media_letter && strcmp (media_letter, " ") &&
      strcmp (media_letter, cartridge.m_media_letter))
    RETURN (EINVAL);

  sbp = repbuf;
  marshall_STRING (sbp, cartridge.m_media_letter);
  if (magic < VMGR_MAGIC2)
    marshall_LONG (sbp, 0);  /* Dummy */
  marshall_LONG (sbp, cartridge.media_cost);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_querytape - query about a tape volume */

int vmgr_srv_querytape(const int magic,
                       char *const req_data,
                       struct vmgr_srv_thread_info *const thip,
                       struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_dgnmap dgnmap_entry;
  char *func = "querytape";
  char *rbp = NULL;
  char repbuf[177];
  char *sbp = NULL;
  int side = 0;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_info_byte_u64 tape;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);
  if (magic >= VMGR_MAGIC2) {
    unmarshall_WORD (rbp, side);
  } else
    side = 0;

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s Side=%d", vid, side);

  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 0, NULL))
    RETURN (serrno);

  /* Get side specific info */
  if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, side, &side_entry,
					0, NULL))
    RETURN (serrno);

  /* Get dgn */
  if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
			     &dgnmap_entry, 0, NULL))
    RETURN (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, tape.vsn);
  if (magic >= VMGR_MAGIC2) {
    marshall_STRING (sbp, tape.library);
  }
  marshall_STRING (sbp, dgnmap_entry.dgn);
  marshall_STRING (sbp, tape.density);
  marshall_STRING (sbp, tape.lbltype);
  marshall_STRING (sbp, tape.model);
  marshall_STRING (sbp, tape.media_letter);
  marshall_STRING (sbp, tape.manufacturer);
  marshall_STRING (sbp, tape.sn);
  if (magic >= VMGR_MAGIC2) {
    marshall_WORD (sbp, tape.nbsides);
    marshall_TIME_T (sbp, tape.etime);
    marshall_WORD (sbp, side_entry.side);
  }
  marshall_STRING (sbp, side_entry.poolname);
  {
    /* Give the estimated free-space in kibibytes a ceiling of the */
    /* largest posible positive 32-bit signed integer which is     */
    /* 2^32-1 = 2147483647                                         */
    int estimated_free_space_kibibyte_int = 0;
    if(side_entry.estimated_free_space_byte_u64 / ONE_KB > 2147483647) {
      estimated_free_space_kibibyte_int = 2147483647;
    } else {
      estimated_free_space_kibibyte_int =
        side_entry.estimated_free_space_byte_u64 / ONE_KB;
    }
    marshall_LONG (sbp, estimated_free_space_kibibyte_int);
  }
  marshall_LONG (sbp, side_entry.nbfiles);
  marshall_LONG (sbp, tape.rcount);
  marshall_LONG (sbp, tape.wcount);
  if (magic >= VMGR_MAGIC2) {
    marshall_STRING (sbp, tape.rhost);
    marshall_STRING (sbp, tape.whost);
    marshall_LONG (sbp, tape.rjid);
    marshall_LONG (sbp, tape.wjid);
  }
  marshall_TIME_T (sbp, tape.rtime);
  marshall_TIME_T (sbp, tape.wtime);
  marshall_LONG (sbp, side_entry.status);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_querytape - query about a tape volume */

int vmgr_srv_querytape_byte_u64(const int magic,
				char *const req_data,
				struct vmgr_srv_thread_info *const thip,
                                struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_dgnmap dgnmap_entry;
  char *func = "querytape";
  char *rbp = NULL;
  char repbuf[177];
  char *sbp = NULL;
  int side = 0;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_info_byte_u64 tape;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  /* Only VMGR_MAGIC2 is supported */
  if (magic != VMGR_MAGIC2)
    RETURN (EINVAL);

  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s Side=%d", vid, side);

  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 0, NULL))
    RETURN (serrno);

  /* Get side specific info */
  if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, side, &side_entry,
					0, NULL))
    RETURN (serrno);

  /* Get dgn */
  if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
			     &dgnmap_entry, 0, NULL))
    RETURN (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, tape.vsn);
  marshall_STRING (sbp, tape.library);
  marshall_STRING (sbp, dgnmap_entry.dgn);
  marshall_STRING (sbp, tape.density);
  marshall_STRING (sbp, tape.lbltype);
  marshall_STRING (sbp, tape.model);
  marshall_STRING (sbp, tape.media_letter);
  marshall_STRING (sbp, tape.manufacturer);
  marshall_STRING (sbp, tape.sn);
  marshall_WORD (sbp, tape.nbsides);
  marshall_TIME_T (sbp, tape.etime);
  marshall_WORD (sbp, side_entry.side);
  marshall_STRING (sbp, side_entry.poolname);
  marshall_HYPER (sbp, side_entry.estimated_free_space_byte_u64);
  marshall_LONG (sbp, side_entry.nbfiles);
  marshall_LONG (sbp, tape.rcount);
  marshall_LONG (sbp, tape.wcount);
  marshall_STRING (sbp, tape.rhost);
  marshall_STRING (sbp, tape.whost);
  marshall_LONG (sbp, tape.rjid);
  marshall_LONG (sbp, tape.wjid);
  marshall_TIME_T (sbp, tape.rtime);
  marshall_TIME_T (sbp, tape.wtime);
  marshall_LONG (sbp, side_entry.status);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*  vmgr_srv_reclaim - reset tape volume content information */

int vmgr_srv_reclaim(char *req_data,
                     struct vmgr_srv_thread_info *thip,
                     struct vmgr_srv_request_info *reqinfo)
{
  struct vmgr_tape_denmap_byte_u64 denmap_entry;
  char *func = "reclaim";
  int i;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addrp;
  vmgr_dbrec_addr rec_addrs;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_info_byte_u64 tape;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s", vid);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entries */
  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 0, NULL))
    RETURN (serrno);

  memset ((char *) &denmap_entry, 0, sizeof(denmap_entry));
  if (vmgr_get_denmap_entry_byte_u64 (&thip->dbfd, tape.model,
				      tape.media_letter, tape.density,
				      &denmap_entry, 0, NULL))
    RETURN (serrno);

  /* Update entries */
  memset((void *) &pool_entry, 0, sizeof(pool_entry));
  for (i = 0; i < tape.nbsides; i++) {
    if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, i, &side_entry,
					  1, &rec_addrs))
      RETURN (serrno);
    if (i == 0 && vmgr_get_pool_entry_byte_u64 (&thip->dbfd,
						side_entry.poolname,
						&pool_entry, 1, &rec_addrp))
      RETURN (serrno);

    side_entry.estimated_free_space_byte_u64 =
      denmap_entry.native_capacity_byte_u64;
    side_entry.nbfiles = 0;
    side_entry.status = 0;

    if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addrs, &side_entry))
      RETURN (serrno);

    pool_entry.tot_free_space_byte_u64 += denmap_entry.native_capacity_byte_u64;
  }
  if (vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &rec_addrp, &pool_entry))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_settag - add/replace a tag associated with a tape volume */

int vmgr_srv_settag(char *req_data,
                    struct vmgr_srv_thread_info *thip,
                    struct vmgr_srv_request_info *reqinfo)
{
  char *func = "settag";
  struct vmgr_tape_tag old_tag_entry;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_tag tag_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  memset ((char *) &tag_entry, 0, sizeof(tag_entry));
  if (unmarshall_STRINGN (rbp, tag_entry.vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, tag_entry.text, CA_MAXTAGLEN+1) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s Text=\"%s\"",
           tag_entry.vid, tag_entry.text);

  /* Get pool name */
  if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, tag_entry.vid, 0,
					&side_entry, 0, NULL))
    RETURN (serrno);

  /* Get pool entry */
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
				    &pool_entry, 0, NULL))
    RETURN (serrno);

  /* Check if the user is authorized to add/replace the tag on this volume */
  if (((pool_entry.uid && pool_entry.uid != reqinfo->uid) ||
       (pool_entry.gid && pool_entry.gid != reqinfo->gid)) &&
      Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_TAPE_OPERATOR))
    RETURN (EACCES);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Add the tag entry or replace the tag text if the entry exists */
  if (vmgr_insert_tag_entry (&thip->dbfd, &tag_entry)) {
    if (serrno != EEXIST ||
        vmgr_get_tag_by_vid (&thip->dbfd, tag_entry.vid,
			     &old_tag_entry, 1, &rec_addr) ||
        vmgr_update_tag_entry (&thip->dbfd, &rec_addr, &tag_entry))
      RETURN (serrno);
  }
  RETURN (0);
}

/*  vmgr_srv_tpmounted - update tape volume content information */

int vmgr_srv_tpmounted(int magic,
                       char *req_data,
                       struct vmgr_srv_thread_info *thip,
                       struct vmgr_srv_request_info *reqinfo)
{
  char *func = "tpmounted";
  char hostname[CA_MAXHOSTNAMELEN+1];
  int jid;
  int mode;
  char *p;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  struct vmgr_tape_info_byte_u64 tape;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, mode);
  if (magic >= VMGR_MAGIC2) {
    unmarshall_LONG (rbp, jid);
  } else
    jid = 0;

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s Mode=%d", vid, mode);

  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost, localhost,
                  P_TAPE_SYSTEM))
    RETURN (serrno);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entry */
  memset((void *) &tape, 0, sizeof(tape));
  if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 1, &rec_addr))
    RETURN (serrno);

  /* Update entry */
  strcpy (hostname, reqinfo->clienthost);
  if ((p = strchr (hostname, '.'))) *p = '\0';
  *(hostname + CA_MAXSHORTHOSTLEN) = '\0';
  if (mode) {
    tape.wcount++;
    strcpy (tape.whost, hostname);
    tape.wjid = jid;
    tape.wtime = time (0);
  } else {
    tape.rcount++;
    strcpy (tape.rhost, hostname);
    tape.rjid = jid;
    tape.rtime = time (0);
  }

  if (vmgr_update_tape_entry_byte_u64 (&thip->dbfd, &rec_addr, &tape))
    RETURN (serrno);
  RETURN (0);
}

/*  vmgr_srv_updatetape - update tape volume content information */

int vmgr_srv_updatetape(int magic,
                        char *req_data,
                        struct vmgr_srv_thread_info *thip,
                        struct vmgr_srv_request_info *reqinfo)
{
  u_signed64 BytesWritten = 0;
  int CompressionFactor;
  char *func = "updatetape";
  int FilesWritten = 0;
  int Flags = 0;
  int i;
  u_signed64 normalized_data_written_byte_u64 = 0;
  struct vmgr_tape_pool_byte_u64 pool_entry;
  char *rbp;
  vmgr_dbrec_addr rec_addr;
  vmgr_dbrec_addr rec_addrp;
  int side;
  struct vmgr_tape_side_byte_u64 side_entry;
  struct vmgr_tape_side_byte_u64 side_entry1;
  struct vmgr_tape_info_byte_u64 tape;
  char tmpbuf[21];
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
    RETURN (EINVAL);
  if (magic >= VMGR_MAGIC2) {
    unmarshall_WORD (rbp, side);
  } else
    side = 0;
  unmarshall_HYPER (rbp, BytesWritten);
  unmarshall_WORD (rbp, CompressionFactor);
  unmarshall_WORD (rbp, FilesWritten);
  unmarshall_WORD (rbp, Flags);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "TPVID=%s Side=%d BytesWritten=%s CompressionFactor=%d "
           "FilesWritten=%d Flags=%d",
	   vid, side,
	   u64tostr(BytesWritten, tmpbuf, 0),
	   (int) CompressionFactor,
	   (int) FilesWritten,
	   (int) Flags);

  /* Start transaction */
  (void) vmgr_start_tr (&thip->dbfd);

  /* Get and lock entries */
  memset((void *) &side_entry, 0, sizeof(side_entry));
  if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, side, &side_entry, 1,
					&rec_addr))
    RETURN (serrno);

  memset((void *) &pool_entry, 0, sizeof(pool_entry));
  if (vmgr_get_pool_entry_byte_u64 (&thip->dbfd, side_entry.poolname,
				    &pool_entry, 1, &rec_addrp))
    RETURN (serrno);

  /* Calculate the normalised amount of data written taking into account */
  /* the compression factor                                              */
  if(BytesWritten == 0) {
    normalized_data_written_byte_u64 = 0;
  } else {
    if (CompressionFactor == 0) {
      normalized_data_written_byte_u64 = BytesWritten;
    } else {
      normalized_data_written_byte_u64 = BytesWritten * 100 / CompressionFactor;
    }
  }

  /* Apply a ceiling of the estimated amount of free space */
  if(normalized_data_written_byte_u64 >
     side_entry.estimated_free_space_byte_u64) {
    normalized_data_written_byte_u64 = side_entry.estimated_free_space_byte_u64;
  }

  /* Update the estimated amount of free space on the tape */
  side_entry.estimated_free_space_byte_u64 -= normalized_data_written_byte_u64;

  /* Update the number of files written and the status of the tape */
  side_entry.nbfiles += FilesWritten;
  side_entry.status &= ~TAPE_BUSY;
  side_entry.status |= Flags;
  if (side_entry.estimated_free_space_byte_u64 == 0) {
    side_entry.status |= TAPE_FULL;
  }

  if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addr, &side_entry))
    RETURN (serrno);

  if ((Flags & TAPE_BUSY) == 0) {  /* reset BUSY flags on all sides */
    if (vmgr_get_tape_by_vid_byte_u64 (&thip->dbfd, vid, &tape, 0, NULL))
      RETURN (serrno);
    for (i = 0; i < tape.nbsides; i++) {
      if (i == side_entry.side) continue;
      if (vmgr_get_side_by_fullid_byte_u64 (&thip->dbfd, vid, i,
					    &side_entry1, 1, &rec_addr))
        RETURN (serrno);
      if ((side_entry1.status & TAPE_BUSY) == 0 && Flags == 0)
        continue;
      side_entry1.status &= ~TAPE_BUSY;
      if (Flags & EXPORTED)
        side_entry1.status |= EXPORTED;
      if (vmgr_update_side_entry_byte_u64 (&thip->dbfd, &rec_addr,
					   &side_entry1))
        RETURN (serrno);
    }
  }

  /* Update the total amount of free space in the tape-pool if data was */
  /* written, making sure the total amount does not go negative         */
  if(normalized_data_written_byte_u64 > 0) {
    if(normalized_data_written_byte_u64 > pool_entry.tot_free_space_byte_u64) {
      pool_entry.tot_free_space_byte_u64 = 0;
    } else {
      pool_entry.tot_free_space_byte_u64 -= normalized_data_written_byte_u64;
    }

    if(vmgr_update_pool_entry_byte_u64 (&thip->dbfd, &rec_addrp, &pool_entry))
      RETURN (serrno);
  }

  RETURN (0);
}
