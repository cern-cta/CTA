# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctadiskroutines
  DiskReportArchiveRoutine.cpp
  DiskReportRetrieveRoutine.cpp)

target_link_libraries(ctadiskroutines
	ctacommon
	ctadisk
	ctascheduler)
