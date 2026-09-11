# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-list cta-objectstore-list.cpp)
target_link_libraries(cta-objectstore-list ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-list DESTINATION ${CMAKE_INSTALL_BINDIR})
