cmake_minimum_required (VERSION 2.6)

add_executable(unittests
  unit_tests.cpp)

target_link_libraries(unittests
  ctacatalogueunittests
  ctaexceptionunittests
  ctaiounittests
  ctalegacymsgunittests
  ctalogunittests
  ctamessagesunittests
  ctaserverunittests
  ctatapereactorunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctatapeserverutilsunittests
  ctautilsunittests
  ctacommon
  ctacommonunittests
  ctaobjectstore
  ctaobjectstoreunittests
  ctanameserverunittests
  ctaremotensunittests
  ctaschedulerunittests
  gmock
  gtest
  pthread)
