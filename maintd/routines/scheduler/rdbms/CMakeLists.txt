# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctardbroutines
		InactiveMountQueueRoutines.cpp
		AncientRowRoutines.cpp
)

target_link_libraries(ctardbroutines
	ctardbms
	ctacatalogue
)

set_property(TARGET ctardbroutines PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctardbroutines PROPERTY   VERSION "${CTA_LIBVERSION}")
install(TARGETS ctardbroutines DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
