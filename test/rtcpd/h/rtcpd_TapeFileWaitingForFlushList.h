/******************************************************************************
 *                h/rtcpd_TapeFileWaitingForFlushList.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef H_RTCPD_TAPEFILEWAITINGFORFLUSHLIST
#define H_RTCPD_TAPEFILEWAITINGFORFLUSHLIST 1

#include "h/osdep.h"
#include "h/rtcpd_TapeFileWaitingForFlush.h"

#include <stdint.h>

typedef struct rtcpd_TapeFileWaitingForFlushList {
  rtcpd_TapeFileWaitingForFlush_t *head;
  rtcpd_TapeFileWaitingForFlush_t *tail;

  uint32_t nbElements;
} rtcpd_TapeFileWaitingForFlushList_t;

/**
 * Initilises the specified list to contain no elements.  Do not call this
 * function with a list that alrady contains elements as doing so will cause a
 * memory leak.
 *
 * @param  list The list to be initialised.
 * @return      0 on success and -1 on failure.  serrno is set on failure.
 */
EXTERN_C int rtcpd_initTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list);

/**
 * Appends a new element iwith the specified value to the end of the specified
 * list.
 *
 * @param list The list to be appended to.
 * @return     0 on success and -1 on failure.  serrno is set on failure.
 */
EXTERN_C int rtcpd_appendToTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list, 
  const int value);

/**
 * Frees all of the memory used to store the elements of the specified list.
 * Please note that the list structure is not freed from memory.  The list
 * structure is updated accordingly, both head and tail are set to NULL and the
 * number of elements (nbElements) is reset to 0.
 *
 * It is safe to pass an initialised empty list to this function.  In this
 * case no elements will be freed and the list structure will be left as is.
 *
 * @param list The list whose elements are to be freed from memory.
 * @return     0 on success and -1 on failure.  serrno is set on failure.
 */
EXTERN_C int rtcpd_freeTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list);

#endif /* H_RTCPD_TAPEFILEWAITINGFORFLUSHLIST */
