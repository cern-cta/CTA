/*
 * SPDX-FileCopyrightText: 1998 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "spectra_like_libs.hpp"

#include <ctype.h>
#include <string.h>

#define N_LIBS (sizeof(spectra_like_libs) / sizeof(spectra_like_libs[0]))

struct vendor_product_id_pair {
  char vendor_id[9];
  char product_id[17];
};

static struct vendor_product_id_pair const spectra_like_libs[] = {
  /* To match all products of a specific vendor, set the product_id as empty string */
  {"SPECTRA", ""       }, /*Match all SPECTRA libraries */
  {"IBM",     "3573-TL"}  /*Match IBM libraries, model 3573-TL*/
};

void trim_trailing_spaces(char* str, int strSize) {
  if (strSize == 0) {
    return;
  }
  str[strSize - 1] = '\0';
  if (strSize == 1) {
    return;
  }
  for (int pos = strSize - 2; pos >= 0 && isspace(str[pos]); --pos) {
    str[pos] = '\0';
  }
}

int is_library_spectra_like(const struct robot_info* const robot_info) {
  int isSpectraLike;
  char vendorId[9];
  char productId[17];
  unsigned int i;

  memcpy(vendorId, robot_info->inquiry, 8);
  memcpy(productId, robot_info->inquiry + 8, 16);

  /* Trim any whitespaces in excess */
  trim_trailing_spaces(vendorId, sizeof(vendorId));
  trim_trailing_spaces(productId, sizeof(productId));

  /* find if the vendor (and product) are in the list 'spectra_like_libs' */
  isSpectraLike = 0;
  for (i = 0; i < N_LIBS && !isSpectraLike; i++) {
    if (strcasecmp(vendorId, spectra_like_libs[i].vendor_id) == 0
        && (spectra_like_libs[i].product_id[0] == '\0'
            || strcasecmp(productId, spectra_like_libs[i].product_id) == 0)) {
      isSpectraLike = 1;
    }
  }

  return isSpectraLike;
}
