# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-initialize cta-objectstore-initialize.cpp)
target_link_libraries(cta-objectstore-initialize ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-initialize DESTINATION ${CMAKE_INSTALL_BINDIR})
