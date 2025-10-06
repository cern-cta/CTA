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

add_library(ctarepackrequestrunner 
	RepackRequestManager.cpp 
	RepackReportThread.cpp)

target_link_libraries(ctarepackrequestrunner
	ctacommon
        ctascheduler)

#set_property(TARGET ctarepackrequestrunner PROPERTY SOVERSION "${cta_soversion}")
#set_property(TARGET ctarepackrequestrunner PROPERTY   VERSION "${cta_libversion}")
#install(TARGETS ctarepackrequestrunner DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

#add_library(ctarepackreportrunnerunittests SHARED
#	DiskReportRunnerTests.cpp
#)

#set_property(TARGET ctarepackreport PROPERTY SOVERSION "${cta_soversion}")
#set_property(TARGET ctarepackreport PROPERTY   VERSION "${cta_libversion}")
#install(TARGETS ctarepackreportunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
