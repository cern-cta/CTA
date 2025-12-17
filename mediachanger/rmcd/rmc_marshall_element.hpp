/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/librmc/smc_struct.hpp"

int rmc_marshall_element(char** const sbpp, const struct smc_element_info* const element_info);
