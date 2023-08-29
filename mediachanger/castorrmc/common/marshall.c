/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2000-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <string.h>
#include "marshall.h"
#include "osdep.h"

int unmarshall_STRINGN(char** ptr, const char* ptr_end, char* str, int str_maxlen) {
  if(*ptr+str_maxlen > ptr_end) {
    str_maxlen = ptr_end-*ptr+1;
  }

  strncpy(str, *ptr, str_maxlen);
  int str_len = strnlen(str, str_maxlen);
  if(str_len < str_maxlen) {
    *ptr += str_len+1;
    return 0;
  } else {
    str[str_maxlen-1] = '\0';
    *ptr += strnlen(*ptr, ptr_end-*ptr);
    if(**ptr == '\0') ++*ptr;
    return -1;
  }
}
