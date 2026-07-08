# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctamountdecisionroutines
        MountDecisionRoutine.cpp)

target_link_libraries(ctamountdecisionroutines
  ctacommon
  ctamountdecision)
