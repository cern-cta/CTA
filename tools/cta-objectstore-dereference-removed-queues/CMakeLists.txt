# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-objectstore-dereference-removed-queues cta-objectstore-dereference-removed-queues.cpp)
target_link_libraries(cta-objectstore-dereference-removed-queues ctaobjectstore ctacommon)

install(TARGETS cta-objectstore-dereference-removed-queues DESTINATION ${CMAKE_INSTALL_BINDIR})
