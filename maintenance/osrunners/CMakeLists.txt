# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2021-2025 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

add_library(ctaosdbrunners SHARED
	QueueCleanupRunner.cpp
	GarbageCollector.cpp
)

target_link_libraries(ctaosdbrunners
	ctaobjectstore
	ctacatalogue
)

set_property(TARGET ctaosdbrunners PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaosdbrunners PROPERTY   VERSION "${CTA_LIBVERSION}")
install(TARGETS ctaosdbrunners DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library(ctaosdbrunnersunittests SHARED
        GarbageCollectorTest.cpp
        QueueCleanupRunnerTestUtils.cpp
        QueueCleanupRunnerTest.cpp
        QueueCleanupRunnerConcurrentTest.cpp
)

target_link_libraries(ctaosdbrunnersunittests ctaosdbrunners)
set_property(TARGET ctaosdbrunnersunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaosdbrunnersunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaosdbrunnersunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

