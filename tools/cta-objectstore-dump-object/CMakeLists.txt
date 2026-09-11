# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-dump-object cta-objectstore-dump-object.cpp)
target_link_libraries(cta-objectstore-dump-object ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-dump-object DESTINATION ${CMAKE_INSTALL_BINDIR})
