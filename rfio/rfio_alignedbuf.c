/* RFIO O_DIRECT memory aligned buffer support */
/* 2006/12/08 KELEMEN Peter <Peter.Kelemen@cern.ch> CERN IT/FIO/LA */

/* $Id: rfio_alignedbuf.c,v 1.2 2006/12/08 14:31:53 fuji Exp $ */

#include <unistd.h>		/* getpagesize() */
#include "log.h"
#include "rfio_alignedbuf.h"

#define ALIGNEDBUF_LOG_LEVEL    LOG_INFO

typedef struct a_map_item {
	void *unaligned;
	void *aligned;
	struct a_map_item *next;
} a_map;

static a_map *aligned_mapping = NULL;

/* get address of tail item, NULL if list is empty */
static a_map 
*_a_map_get_tail(void)
{
	a_map *p = NULL;
	a_map *q = aligned_mapping;

	while (q) {
		p = q;
		q = q->next;
	}
	return p;
}

/* get unaligned address given an aligned address, NULL if not found */
static a_map
*a_map_find_unaligned(void *a)
{
	a_map *p = aligned_mapping;
	while (p && p->aligned != a) {
		p = p->next;
	}
	return p;
}

/* append unaligned/aligned address pair at the end of list, complain if
 * mapping already exists; Return aligned address or NULL if list item
 * cannot be allocated */
static void
*a_map_add(void *u, void *a)
{
	a_map *p, *q;
	if ( p = (a_map*)malloc(sizeof(a_map)) ) {
		p->unaligned = u;
		p->aligned = a;
		p->next = NULL;

		log(ALIGNEDBUF_LOG_LEVEL,"%s: u=%p a=%p\n", __func__, u, a);
		if (q = a_map_find_unaligned(a)) {
			log(LOG_ERR,"%s: mapping already exists!\n", __func__);
			return a;
		}
		if (q = _a_map_get_tail()) {
			q->next = p;
			log(ALIGNEDBUF_LOG_LEVEL, "tail q=%p\n", q);
		} else {
			aligned_mapping = p;
			log(ALIGNEDBUF_LOG_LEVEL, "head p=%p\n", p);
		}
		return a;
	} else {
		log(LOG_ERR,"%s: malloc() failed\n", __func__);
		return NULL;
	}
}

/* delete mapping given an aligned address, return unaligned address, NULL if
 * not found */
static void
*a_map_del(void *a)
{
	void *u = NULL;
	a_map *p = NULL;
	a_map *q = NULL;

	q = aligned_mapping;		/* walk with q */
	while (q && q->aligned != a) {
		p = q;			/* keep track of previous item */
		q = q->next;
	}
	if (q) {
		if (p) {
			p->next = q->next;
		} else if (q->next) {
			aligned_mapping = q->next;
		} else {
			aligned_mapping = NULL;
		}
		u = q->unaligned;
		free(q);
		log(ALIGNEDBUF_LOG_LEVEL,"%s: u=%p a=%p\n", __func__, u, a);
	} else {
		log(LOG_ERR,"%s: a=%p mapping not found!\n", __func__, a);
	}
	return u;
}

void 
*malloc_page_aligned(size_t size)
{
	unsigned long page_size, mask;
	int padding;
	void *unaligned, *aligned;

	log(ALIGNEDBUF_LOG_LEVEL,"%s(%ld)\n", __func__, size);
	page_size = getpagesize();
	mask = page_size-1;
	padding = (size + mask) / page_size;
	padding *= page_size;
	unaligned = malloc(padding + mask);
	if (unaligned) {
		aligned = (void*)((intptr_t) (unaligned + mask) & ~mask);
		if ( a_map_add(unaligned, aligned) ) {
			log(ALIGNEDBUF_LOG_LEVEL, "%s: success\n", __func__);
			return (void*)aligned;
		} else {
			return NULL;
		}
	} else {
		log(ALIGNEDBUF_LOG_LEVEL, "%s: failure\n", __func__);
		return NULL;
	}
}

void
free_page_aligned(void *buf)
{
	void *unaligned;

	log(ALIGNEDBUF_LOG_LEVEL,"%s: buf=%p\n", __func__, buf);
	unaligned = a_map_del(buf);
	if (unaligned) {
		free(unaligned);
		log(ALIGNEDBUF_LOG_LEVEL, "%s: success\n", __func__);
	} else {
		log(ALIGNEDBUF_LOG_LEVEL, "%s: failure\n", __func__);
	}
}

/* eof */
