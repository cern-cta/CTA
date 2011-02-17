#include "time.h"
#include <unistd.h>
#include <sys/types.h>

#include "Castor_limits_trunk_r21843.h"
#include "osdep_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"

#include <stdio.h>

#define MARSHALLED_TAPE_ENTRYSZ (CA_MAXVIDLEN+1+CA_MAXVSNLEN+1+CA_MAXTAPELIBLEN+1+CA_MAXDENLEN+1+CA_MAXLBLTYPLEN+1+CA_MAXMODELLEN+1+CA_MAXMLLEN+1+CA_MAXMANUFLEN+1+CA_MAXSNLEN+1+WORDSIZE+TIME_TSIZE+WORDSIZE+CA_MAXPOOLNAMELEN+1+LONGSIZE+LONGSIZE+LONGSIZE+LONGSIZE+CA_MAXSHORTHOSTLEN+1+CA_MAXSHORTHOSTLEN+1+LONGSIZE+LONGSIZE+TIME_TSIZE+TIME_TSIZE+LONGSIZE)

#define MARSHALLED_TAPE_BYTE_U64_ENTRYSZ    \
(                                           \
  /* vid          */ CA_MAXVIDLEN+1       + \
  /* vsn          */ CA_MAXVSNLEN+1       + \
  /* library      */ CA_MAXTAPELIBLEN+1   + \
  /* density      */ CA_MAXDENLEN+1       + \
  /* lbltype      */ CA_MAXLBLTYPLEN+1    + \
  /* model        */ CA_MAXMODELLEN+1     + \
  /* media_letter */ CA_MAXMLLEN+1        + \
  /* manufacturer */ CA_MAXMANUFLEN+1     + \
  /* sn           */ CA_MAXSNLEN+1        + \
  /* nbsides      */ WORDSIZE             + \
  /* etime        */ TIME_TSIZE           + \
  /* side         */ WORDSIZE             + \
  /* poolname     */ CA_MAXPOOLNAMELEN+1  + \
  /* free_space   */ HYPERSIZE            + \
  /* nbfiles      */ LONGSIZE             + \
  /* rcount       */ LONGSIZE             + \
  /* wcount       */ LONGSIZE             + \
  /* rhost        */ CA_MAXSHORTHOSTLEN+1 + \
  /* whost        */ CA_MAXSHORTHOSTLEN+1 + \
  /* rjid         */ LONGSIZE             + \
  /* wjid         */ LONGSIZE             + \
  /* rtime        */ TIME_TSIZE           + \
  /* wtime        */ TIME_TSIZE           + \
  /* status       */ LONGSIZE               \
)


struct vmgr_tape_info_byte_u64 {
	char		vid[CA_MAXVIDLEN+1];
	char		vsn[CA_MAXVSNLEN+1];
	char		library[CA_MAXTAPELIBLEN+1];
	char		density[CA_MAXDENLEN+1];
	char		lbltype[CA_MAXLBLTYPLEN+1];
	char		model[CA_MAXMODELLEN+1];
	char		media_letter[CA_MAXMLLEN+1];
	char		manufacturer[CA_MAXMANUFLEN+1];
	char		sn[CA_MAXSNLEN+1];	/* serial number */
	int		nbsides;
	time_t		etime;
	int		rcount;
	int		wcount;
	char		rhost[CA_MAXSHORTHOSTLEN+1];
	char		whost[CA_MAXSHORTHOSTLEN+1];
	int		rjid;
	int		wjid;
	time_t		rtime;		/* last access to tape in read mode */
	time_t		wtime;		/* last access to tape in write mode */
	int		side;
	char		poolname[CA_MAXPOOLNAMELEN+1];
	short		status;		/* TAPE_FULL, DISABLED, EXPORTED */
	u_signed64	estimated_free_space_byte_u64;
	int		nbfiles;
};

struct vmgr_tape_library {
	char		name[CA_MAXTAPELIBLEN+1];
	int		capacity;	/* number of slots */
	int		nb_free_slots;
	short		status;
};

struct vmgr_tape_media {
	char		m_model[CA_MAXMODELLEN+1];
	char		m_media_letter[CA_MAXMLLEN+1];
	int		media_cost;
};
 
struct vmgr_tape_denmap_byte_u64 {
	char		md_model[CA_MAXMODELLEN+1];
	char		md_media_letter[CA_MAXMLLEN+1];
	char		md_density[CA_MAXDENLEN+1];
	u_signed64	native_capacity_byte_u64;
};

struct vmgr_tape_dgnmap {
	char		dgn[CA_MAXDGNLEN+1];
	char		model[CA_MAXMODELLEN+1];
	char		library[CA_MAXTAPELIBLEN+1];
};

struct vmgr_tape_dgn {
	char		dgn[CA_MAXDGNLEN+1];
	int		weight;
	int		deltaweight;
};

struct vmgr_tape_pool_byte_u64 {
	char		name[CA_MAXPOOLNAMELEN+1];
	uid_t		uid;
	gid_t		gid;
	u_signed64	capacity_byte_u64;
	u_signed64	tot_free_space_byte_u64;
};


int main() {
  const int denmap_listentsz  = sizeof(struct vmgr_tape_denmap_byte_u64);
  const int dgnmap_listentsz  = sizeof(struct vmgr_tape_dgnmap);
  const int library_listentsz = sizeof(struct vmgr_tape_library);
  const int model_listentsz   = sizeof(struct vmgr_tape_media);
  const int pool_listentsz    = sizeof(struct vmgr_tape_pool_byte_u64);
  const int tape32_listentsz  = MARSHALLED_TAPE_ENTRYSZ;
  const int tape64_listentsz  = MARSHALLED_TAPE_BYTE_U64_ENTRYSZ;

  const int denmap_maxnbentries  = LISTBUFSZ / denmap_listentsz;
  const int dgnmap_maxnbentries  = LISTBUFSZ / dgnmap_listentsz;
  const int library_maxnbentries = LISTBUFSZ / library_listentsz;
  const int model_maxnbentries   = LISTBUFSZ / model_listentsz;
  const int pool_maxnbentries    = LISTBUFSZ / pool_listentsz;
  const int tape32_maxnbentries  = LISTBUFSZ / tape32_listentsz;
  const int tape64_maxnbentries  = LISTBUFSZ / tape64_listentsz;

  printf("denmap_maxnbentries  = %d\n", denmap_maxnbentries);
  printf("dgnmap_maxnbentries  = %d\n", dgnmap_maxnbentries);
  printf("library_maxnbentries = %d\n", library_maxnbentries);
  printf("model_maxnbentries   = %d\n", model_maxnbentries);
  printf("pool_maxnbentries    = %d\n", pool_maxnbentries);
  printf("tape32_maxnbentries  = %d\n", tape32_maxnbentries);
  printf("tape64_maxnbentries  = %d\n", tape64_maxnbentries);

  return 0;
}
