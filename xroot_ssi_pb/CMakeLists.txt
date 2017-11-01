set( CTA_SSI_HEADERS
  XrdSsiPbAlert.hpp
  XrdSsiPbDebug.hpp
  XrdSsiPbException.hpp
  XrdSsiPbRequest.hpp
  XrdSsiPbRequestProc.hpp
  XrdSsiPbService.hpp
  XrdSsiPbServiceClientSide.hpp
)

install (
  FILES ${CTA_SSI_HEADERS}
  DESTINATION usr/${CMAKE_INSTALL_INCLUDEDIR}/cta
)
