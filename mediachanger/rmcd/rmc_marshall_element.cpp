/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rmc_marshall_element.hpp"

#include "mediachanger/librmc/marshall.hpp"

int rmc_marshall_element(char** const sbpp, const struct smc_element_info* const element_info) {
  char* sbp = *sbpp;

  marshall_SHORT(sbp, element_info->element_address);
  marshall_BYTE(sbp, element_info->element_type);
  marshall_BYTE(sbp, element_info->state);
  marshall_BYTE(sbp, element_info->asc);
  marshall_BYTE(sbp, element_info->ascq);
  marshall_BYTE(sbp, element_info->flags);
  marshall_SHORT(sbp, element_info->source_address);
  marshall_STRING(sbp, element_info->name);
  *sbpp = sbp;
  return 0;
}
