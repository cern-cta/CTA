/* RFIO O_DIRECT memory aligned buffer support */
/* 2006/02/15 KELEMEN Peter <Peter.Kelemen@cern.ch> CERN IT/FIO/LA */

#ifndef RFIO_ALIGNEDBUF_H
#define RFIO_ALIGNEDBUF_H

/* NOTE(fuji): In order to be able to use O_DIRECT, we have to allocate
 * page-aligned memory buffers.  The technique we use for that is allocating
 * more and then find a page boundary and use that as the buffer entry.  We keep
 * unaligned/aligned pointer pairs in a single linked list.  There's no locking,
 * i.e. we assume the list won't be manipulated from different thread contexts.
 */

void *malloc_page_aligned(size_t size);
void free_page_aligned(void *buf);

#endif /* RFIO_ALIGNEDBUF_H */

/* eof */
