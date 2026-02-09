# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_executable(cta-eos-namespace-inject EosNamespaceInjection.cpp EosNamespaceInjectionMain.cpp ../../CtaAdminCmdParser.cpp MetaData.cpp)
target_link_libraries(cta-eos-namespace-inject ctaCmdlineToolsCommon)

install(TARGETS cta-eos-namespace-inject DESTINATION usr/bin)
