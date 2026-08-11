# SPDX-FileCopyrightText: 2018 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


#
# XRootD SSI plugin for CTA Frontend
#
add_library(XrdSsiCta MODULE XrdSsiCtaServiceProvider.cpp XrdSsiCtaRequestProc.cpp XrdSsiCtaRequestMessage.cpp AdminCmdStream.cpp ${PROJECT_SOURCE_DIR}/tools/CtaAdminCmdParser.cpp)

target_include_directories(XrdSsiCta PRIVATE
  ${XROOTD_INCLUDE_DIR}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${XRD_SSI_PB_DIR}/include
  ${XRD_SSI_PB_DIR}/eos_cta/include
  ${CMAKE_BINARY_DIR}/eos_cta
  ${PROTOBUF3_INCLUDE_DIRS}
)

if(CTA_USE_PGSCHED)
  target_link_libraries(XrdSsiCta ${XROOTD_XRDSSI_LIB} XrdSsiLib ctafrontendcommon ctascheduler ctacommon ctacatalogue)
else()
  target_link_libraries(XrdSsiCta ${XROOTD_XRDSSI_LIB} XrdSsiLib ctafrontendcommon ctascheduler ctacommon ctaobjectstore ctacatalogue)
endif()

install(TARGETS XrdSsiCta DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(FILES cta-frontend-xrootd.example.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install(FILES cta-frontend.logrotate DESTINATION /etc/logrotate.d RENAME cta-frontend)
install(FILES cta-frontend.sysconfig DESTINATION /etc/sysconfig RENAME cta-frontend)
install(FILES cta-frontend.service DESTINATION ${CTA_SYSTEMD_UNIT_DIR})
