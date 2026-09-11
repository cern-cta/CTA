# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-reset cta-objectstore-reset.cpp)
target_link_libraries(cta-objectstore-reset ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-reset DESTINATION ${CMAKE_INSTALL_BINDIR})
