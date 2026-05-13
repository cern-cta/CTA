# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_executable(cta-eos-namespace-inject EosNamespaceInjection.cpp EosNamespaceInjectionMain.cpp ../../CtaAdminCmdParser.cpp MetaData.cpp)

# To prevent failing due to warning
# #warning "<ciso646> is not a standard header since C++20, use <version> to detect implementation-specific macros"
target_compile_options(cta-eos-namespace-inject PRIVATE $<$<CXX_COMPILER_ID:GNU>:-Wno-error=cpp>)
target_link_libraries(cta-eos-namespace-inject ctaCmdlineToolsCommon)

install(TARGETS cta-eos-namespace-inject DESTINATION usr/bin)
