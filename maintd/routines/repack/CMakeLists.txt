# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctarepackroutines
    RepackExpandRoutine.cpp
    RepackReportRoutine.cpp)

target_link_libraries(ctarepackroutines
    ctacommon
    ctascheduler)

