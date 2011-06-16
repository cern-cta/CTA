/******************************************************************************
 *                rtcpd/rtcpd_TapeFileWaitingForFlushList.c
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

#include "h/rtcpd_TapeFileWaitingForFlushList.h"
#include "h/serrno.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>

/*---------------------------------------------------------------------------*/
/* rtcpd_initTapeFileWaitingForFlushList                                     */
/*---------------------------------------------------------------------------*/
int rtcpd_initTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list) {
  if(NULL == list) {
    serrno = EINVAL;
    return -1;
  }

  list->head = NULL;
  list->tail = NULL;
  list->nbElements = 0;

  return 0;
}


/*---------------------------------------------------------------------------*/
/* rtcpd_initTapeFileWaitingForFlushList                                     */
/*---------------------------------------------------------------------------*/
int rtcpd_appendToTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list,
  const int value) {
   rtcpd_TapeFileWaitingForFlush_t *element = NULL;

  if(NULL == list ||
    (NULL == list->head && NULL != list->tail) ||
    (NULL != list->head && NULL == list->tail) ||
    (NULL == list->head && NULL == list->tail && list->nbElements != 0)) {
    serrno = EINVAL;
    return -1;
  }

  element = calloc(1, sizeof(rtcpd_TapeFileWaitingForFlush_t));
  if(NULL == element) {
    serrno = ENOMEM;
    return -1;
  }

  element->value = value;
  element->next  = NULL;

  if(NULL == list->head) {
    list->head = element;
    list->tail = list->head;
  } else {
    list->tail->next = element;
    list->tail       = element;
  }

  list->nbElements++;

  return 0;
}


/*---------------------------------------------------------------------------*/
/* rtcpd_initTapeFileWaitingForFlushList                                     */
/*---------------------------------------------------------------------------*/
int rtcpd_freeTapeFileWaitingForFlushList(
  rtcpd_TapeFileWaitingForFlushList_t *const list) {
  rtcpd_TapeFileWaitingForFlush_t *element  = NULL;
  rtcpd_TapeFileWaitingForFlush_t *toDelete = NULL;

  if(NULL == list ||
    (NULL == list->head && NULL != list->tail) ||
    (NULL != list->head && NULL == list->tail) ||
    (NULL == list->head && NULL == list->tail && list->nbElements != 0)) {
    serrno = EINVAL;
    return -1;
  }

  element = list->head;
  while(element != NULL) {
    toDelete = element;
    element = element->next;
    free(toDelete);
  }

  list->head = NULL;
  list->tail = NULL;
  list->nbElements = 0;

  return 0;
}
