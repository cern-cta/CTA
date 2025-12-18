# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctadiskroutines
  DiskReportArchiveRoutine.cpp
  DiskReportRetrieveRoutine.cpp)

target_link_libraries(ctadiskroutines
	ctacommon
	ctadisk
	ctascheduler)

