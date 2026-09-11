# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-create-missing-repack-index cta-objectstore-create-missing-repack-index.cpp)
target_link_libraries(cta-objectstore-create-missing-repack-index ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-create-missing-repack-index DESTINATION ${CMAKE_INSTALL_BINDIR})
