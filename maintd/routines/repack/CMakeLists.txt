# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctarepackroutines
    RepackExpandRoutine.cpp
    RepackReportRoutine.cpp)

target_link_libraries(ctarepackroutines
    ctacommon
    ctascheduler)
