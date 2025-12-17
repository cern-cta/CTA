/*
 * SPDX-FileCopyrightText: 2000 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// structure to be used with Cinitdaemon()/Cinitservice()

struct main_args {
  int argc;
  char** argv;
};

int Cinitdaemon(const char* const name, void (*const wait4child)(int));
