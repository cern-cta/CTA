# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-collect-orphaned-object cta-objectstore-collect-orphaned-object.cpp)
target_link_libraries(cta-objectstore-collect-orphaned-object ctaobjectstore ctacommon ctacatalogue)

install(TARGETS cta-objectstore-collect-orphaned-object DESTINATION ${CMAKE_INSTALL_BINDIR})
