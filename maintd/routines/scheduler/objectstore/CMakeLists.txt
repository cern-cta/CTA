# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctaosdbroutines SHARED
	QueueCleanupRoutine.cpp
	GarbageCollectRoutine.cpp
)

target_link_libraries(ctaosdbroutines
	ctaobjectstore
	ctacatalogue
)

set_property(TARGET ctaosdbroutines PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaosdbroutines PROPERTY   VERSION "${CTA_LIBVERSION}")
install(TARGETS ctaosdbroutines DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
