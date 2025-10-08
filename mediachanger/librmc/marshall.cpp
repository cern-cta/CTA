/*
 * SPDX-FileCopyrightText: 2000 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string.h>
#include "marshall.hpp"

int unmarshall_STRINGN(char** ptr, const char* ptr_end, char* str, size_t str_maxlen) {
  if (*ptr + str_maxlen > ptr_end) {
    str_maxlen = ptr_end - *ptr + 1;
  }

  strncpy(str, *ptr, str_maxlen);
  size_t str_len = strnlen(str, str_maxlen);
  if (str_len < str_maxlen) {
    *ptr += str_len + 1;
    return 0;
  } else {
    str[str_maxlen - 1] = '\0';
    *ptr += strnlen(*ptr, ptr_end - *ptr);
    if (**ptr == '\0') {
      ++*ptr;
    }
    return -1;
  }
}
