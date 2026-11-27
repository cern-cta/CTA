/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveGeneric.hpp"
#include "taped/system/Wrapper.hpp"

#include <chrono>
#include <cstring>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

namespace cta::tape::drive {
namespace {

using testing::_;
using testing::An;
using testing::Return;
using namespace std::chrono_literals;

// Script the actual ioctl boundary, including the driver's cached status, without replacing DriveGeneric logic.
class DriveMediaTest : public testing::Test {
protected:
  testing::StrictMock<System::mockWrapper> m_wrapper;
  std::unique_ptr<DriveLTO> m_drive;
  unsigned m_opens = 0;
  unsigned m_closes = 0;

  void SetUp() override {
    EXPECT_CALL(m_wrapper, open(_, _)).WillRepeatedly([&](const char*, int flags) {
      EXPECT_EQ(O_RDWR | O_NONBLOCK, flags);
      ++m_opens;
      return 7;
    });
    EXPECT_CALL(m_wrapper, close(7)).WillRepeatedly([&](int) {
      ++m_closes;
      return 0;
    });
    SCSI::DeviceInfo device {};
    device.nst_dev = "/dev/nst-test";
    m_drive = std::make_unique<DriveLTO>(device, m_wrapper);
  }

  void
  expectNotReady(std::chrono::steady_clock::time_point deadline, const std::string& reason, ReadinessWaitMode mode) {
    try {
      m_drive->waitUntilReady(deadline, mode);
      FAIL() << "Expected MediaNotReady";
    } catch (const MediaNotReady& ex) {
      EXPECT_THAT(ex.getMessageValue(), testing::HasSubstr(reason));
    }
  }

  static int sense(sg_io_hdr_t* command, uint8_t key, uint8_t asc, uint8_t ascq, bool descriptor = false) {
    command->status = SCSI::Status::CHECK_CONDITION;
    command->driver_status = SCSI::DriverStatus::SENSE;
    std::memset(command->sbp, 0, command->mx_sb_len);
    command->sb_len_wr = descriptor ? 8 : 18;
    command->sbp[0] = descriptor ? 0x72 : 0x70;
    command->sbp[descriptor ? 1 : 2] = key;
    command->sbp[descriptor ? 2 : 12] = asc;
    command->sbp[descriptor ? 3 : 13] = ascq;
    command->sbp[7] = descriptor ? 0 : 10;
    return 0;
  }

  void expectStatus(bool online) {
    EXPECT_CALL(m_wrapper, ioctl(7, MTIOCGET, An<mtget*>())).WillOnce([=](int, unsigned long, mtget* status) {
      *status = {};
      status->mt_gstat = online ? GMT_ONLINE(0xffffffff) : GMT_DR_OPEN(0xffffffff);
      return 0;
    });
  }

  void expectDriverUnloadFailure(int error = ENOMEDIUM) {
    EXPECT_CALL(m_wrapper, ioctl(7, MTIOCTOP, An<mtop*>())).WillOnce([=](int, unsigned long, mtop* command) {
      EXPECT_EQ(MTUNLOAD, command->mt_op);
      errno = error;
      return -1;
    });
  }
};

TEST_F(DriveMediaTest, ReadyProbeRefreshesDriverBeforeMediaAccess) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(SCSI::Commands::TEST_UNIT_READY, command->cmdp[0]);
    EXPECT_EQ(60000, command->timeout);
    return 0;
  });
  expectStatus(true);
  EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia));
  EXPECT_EQ(2, m_opens);
  EXPECT_EQ(1, m_closes);
}

TEST_F(DriveMediaTest, UnloadedMediaDoesNotPollOrReopen) {
  for (const auto& [asc, ascq] : {
         std::pair {0x3a, 0x04},
         {0x04, 0x02}
  }) {
    for (const auto mode : {ReadinessWaitMode::AwaitMedia, ReadinessWaitMode::ExistingMediaOnly}) {
      for (bool descriptor : {false, true}) {
        EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
          .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq, descriptor); });
        expectNotReady(std::chrono::steady_clock::now() + 1s, "TEST UNIT READY", mode);
      }
    }
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, ExistingMediaOnlyStopsImmediatelyOnAbsence) {
  for (bool descriptor : {false, true}) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      return sense(command, 2, 0x3a, 0, descriptor);
    });
    expectNotReady(std::chrono::steady_clock::now() + 5s, "TEST UNIT READY", ReadinessWaitMode::ExistingMediaOnly);
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, AwaitMediaPollsThroughCartridgeDeliveryAndLoading) {
  for (bool descriptor : {false, true}) {
    // The library has accepted the mount, but cartridge delivery and loading are still in progress.
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x3a, 0, descriptor); })
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x04, 1, descriptor); })
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 6, 0x28, 0, descriptor); })
      .WillOnce(Return(0));
    expectStatus(true);
    EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 5s, ReadinessWaitMode::AwaitMedia));
  }
  EXPECT_EQ(3, m_opens);
  EXPECT_EQ(2, m_closes);
}

TEST_F(DriveMediaTest, AwaitMediaTimesOutIfCartridgeNeverArrives) {
  for (bool descriptor : {false, true}) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
      .WillRepeatedly([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x3a, 0, descriptor); });
    expectNotReady(std::chrono::steady_clock::now() + 100ms,
                   "Drive readiness timed out: TEST UNIT READY",
                   ReadinessWaitMode::AwaitMedia);
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, AwaitMediaWithExpiredDeadlineOnlyProbesOnce) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    return sense(command, 2, 0x3a, 0);
  });
  expectNotReady(std::chrono::steady_clock::now() - 1s, "Drive readiness timed out", ReadinessWaitMode::AwaitMedia);
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, UnitAttentionIsRetriedBeforeObservingReadiness) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillOnce([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 6, 0x28, 0); })
    .WillOnce([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x3a, 0); });
  EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::ExistingMediaOnly),
               MediaNotReady);
}

TEST_F(DriveMediaTest, ExpiredBudgetOnlyProbesWithoutRetryOrReopen) {
  for (const auto key : {SCSI::senseKeys::unitAttention, SCSI::senseKeys::notReady}) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      EXPECT_EQ(60000, command->timeout);
      return sense(command, key, key == SCSI::senseKeys::unitAttention ? 0x29 : 0x04, 1);
    });
    expectNotReady(std::chrono::steady_clock::now() - 1s, "Drive readiness timed out", ReadinessWaitMode::AwaitMedia);
  }
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, TemporaryNotReadyPollsUntilMediaCanBeAccessed) {
  for (const auto& [asc, ascq] : {
         std::pair {0x04, 0x01},
         {0x04, 0x04},
         {0x04, 0x07},
         {0x04, 0x13},
         {0x00, 0x16},
         {0x30, 0x03}
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq); })
      .WillOnce(Return(0));
    expectStatus(true);
    EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 5s, ReadinessWaitMode::AwaitMedia));
  }
}

TEST_F(DriveMediaTest, PowerOnSelfTestPollsThroughLoadingUntilReady) {
  for (bool descriptor : {false, true}) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x3e, 0, descriptor); })
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x04, 1, descriptor); })
      .WillOnce(Return(0));
    expectStatus(true);
    EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 5s, ReadinessWaitMode::AwaitMedia));
  }
  EXPECT_EQ(3, m_opens);
  EXPECT_EQ(2, m_closes);
}

TEST_F(DriveMediaTest, PowerOnSelfTestDoesNotRetryAfterDeadline) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    return sense(command, 2, 0x3e, 0);
  });
  expectNotReady(std::chrono::steady_clock::now() - 1s, "Drive readiness timed out", ReadinessWaitMode::AwaitMedia);
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, RecoveredErrorsDoNotEstablishReadiness) {
  for (const auto& [asc, ascq] : {
         std::pair {0x00, 0x17},
         {0x83, 0x83}
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      return sense(command, 1, asc, ascq);
    });
    EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia),
                 exception::Exception);
  }
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, UnrecognizedNotReadyCanRecoverBeforeDeadline) {
  for (const auto& [asc, ascq] : {
         std::pair {0x04, 0x00},
         {0x80, 0x00}
  }) {
    for (bool descriptor : {false, true}) {
      EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
        .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq, descriptor); })
        .WillOnce(Return(0));
      expectStatus(true);
      EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 5s, ReadinessWaitMode::AwaitMedia));
    }
  }
}

TEST_F(DriveMediaTest, UnrecognizedNotReadyStopsAtDeadlineWithoutFailingDrive) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillRepeatedly([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x04, 0); });
  expectNotReady(std::chrono::steady_clock::now() + 100ms,
                 "Drive readiness timed out: TEST UNIT READY",
                 ReadinessWaitMode::AwaitMedia);
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, NonRetryableNotReadyStopsWithoutFailingDrive) {
  for (const auto& [asc, ascq] : {
         std::pair {0x04, 0x03},
         {0x53, 0x00}
  }) {
    for (bool descriptor : {false, true}) {
      EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
        .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq, descriptor); });
      expectNotReady(std::chrono::steady_clock::now() + 5s, "TEST UNIT READY", ReadinessWaitMode::AwaitMedia);
    }
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, HardwareAndMediumErrorsRemainFailures) {
  for (const auto& [key, asc] : {
         std::pair {3, 0x53},
         {4, 0x53},
         {4, 0x44}
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      return sense(command, key, asc, 0);
    });
    try {
      m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia);
      FAIL() << "Expected a drive or command error";
    } catch (const MediaNotReady&) {
      FAIL() << "A medium or hardware error must not be treated as a readiness condition";
    } catch (const exception::Exception&) {}
  }
}

TEST_F(DriveMediaTest, InvalidSenseIsNeverAbsence) {
  for (unsigned variant = 0; variant < 5; ++variant) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      sense(command, 2, 0x3a, 0, variant == 3);
      switch (variant) {
        case 0:
          command->sb_len_wr = 0;
          break;
        case 1:
          command->sb_len_wr = 13;
          break;
        case 2:
          command->sbp[7] = 0;
          break;
        case 3:
          command->sb_len_wr = 7;
          break;
        case 4:
          command->sbp[0] = 0x7f;
          break;
      }
      return 0;
    });
    EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia),
                 exception::Exception);
  }
}

TEST_F(DriveMediaTest, TransportAndDeferredErrorsAreNotAbsence) {
  for (unsigned variant = 0; variant < 3; ++variant) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      sense(command, 2, 0x3a, 0);
      if (variant == 0) {
        command->host_status = 1;
      }
      if (variant == 1) {
        command->driver_status = SCSI::DriverStatus::TIMEOUT;
      }
      if (variant == 2) {
        command->sbp[0] = 0x71;
      }
      return 0;
    });
    EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia),
                 exception::Exception);
  }
}

TEST_F(DriveMediaTest, RefreshedOfflineStatusIsNotReportedAsElapsedTimeout) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce(Return(0));
  expectStatus(false);
  try {
    m_drive->waitUntilReady(std::chrono::steady_clock::now() + 900s, ReadinessWaitMode::AwaitMedia);
    FAIL() << "Expected driver-state error";
  } catch (const exception::Exception& ex) {
    EXPECT_THAT(ex.getMessageValue(), testing::HasSubstr("offline after successful TEST UNIT READY"));
    EXPECT_THAT(ex.getMessageValue(), testing::Not(testing::HasSubstr("900 seconds")));
  }
}

TEST_F(DriveMediaTest, LoadingRetriesKeepFullCommandTimeout) {
  std::vector<unsigned> timeouts;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillOnce([&](int, unsigned long, sg_io_hdr_t* command) {
      timeouts.push_back(command->timeout);
      return sense(command, 2, 4, 1);
    })
    .WillOnce([&](int, unsigned long, sg_io_hdr_t* command) {
      timeouts.push_back(command->timeout);
      return 0;
    });
  expectStatus(true);
  EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia));
  ASSERT_EQ(2, timeouts.size());
  EXPECT_EQ(60000, timeouts.front());
  EXPECT_EQ(timeouts.front(), timeouts.back());
}

TEST_F(DriveMediaTest, LoadingDeadlineStopsRetriesWithoutShorteningCommandTimeout) {
  unsigned calls = 0;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillRepeatedly([&](int, unsigned long, sg_io_hdr_t* command) {
      EXPECT_EQ(60000, command->timeout);
      ++calls;
      return sense(command, 2, 4, 1);
    });
  expectNotReady(std::chrono::steady_clock::now() + 1s,
                 "Drive readiness timed out: TEST UNIT READY",
                 ReadinessWaitMode::AwaitMedia);
  EXPECT_GT(calls, 1);
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, ZeroBudgetMakesOneFullTimeoutProbeWithoutRetry) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(60000, command->timeout);
    return sense(command, 6, 0x29, 0);
  });
  expectNotReady(std::chrono::steady_clock::now(), "Drive readiness timed out", ReadinessWaitMode::AwaitMedia);
}

TEST_F(DriveMediaTest, SuccessfulProbeAfterDeadlineRefreshesDriver) {
  const auto deadline = std::chrono::steady_clock::now() + 20ms;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(60000, command->timeout);
    std::this_thread::sleep_until(deadline + 10ms);
    return 0;
  });
  expectStatus(true);
  EXPECT_NO_THROW(m_drive->waitUntilReady(deadline, ReadinessWaitMode::AwaitMedia));
  EXPECT_EQ(2, m_opens);
  EXPECT_EQ(1, m_closes);
}

TEST_F(DriveMediaTest, UnreadyProbeAfterDeadlineDoesNotRetryOrRefreshDriver) {
  const auto deadline = std::chrono::steady_clock::now() + 20ms;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(60000, command->timeout);
    std::this_thread::sleep_until(deadline + 10ms);
    return sense(command, 2, 0x04, 1);
  });
  expectNotReady(deadline, "Drive readiness timed out", ReadinessWaitMode::AwaitMedia);
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, ExpiredDeadlineStillAcceptsReadyMedia) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(60000, command->timeout);
    return 0;
  });
  expectStatus(true);
  EXPECT_NO_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() - 1s, ReadinessWaitMode::AwaitMedia));
  EXPECT_EQ(2, m_opens);
  EXPECT_EQ(1, m_closes);
}

TEST_F(DriveMediaTest, DriverUnloadUsesOnlyMtUnload) {
  EXPECT_CALL(m_wrapper, ioctl(7, MTIOCTOP, An<mtop*>())).WillOnce([](int, unsigned long, mtop* command) {
    EXPECT_EQ(MTUNLOAD, command->mt_op);
    EXPECT_EQ(1, command->mt_count);
    return 0;
  });
  EXPECT_NO_THROW(m_drive->unloadTape());
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, DriverUnloadFailurePropagatesWithoutRecovery) {
  for (int error : {ENOMEDIUM, EIO, EBUSY}) {
    expectDriverUnloadFailure(error);
    try {
      m_drive->unloadTape();
      FAIL() << "Expected unload failure";
    } catch (const exception::Errnum& ex) {
      EXPECT_EQ(error, ex.errorNumber());
    }
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, ReadinessRefreshCloseFailureDoesNotDoubleClose) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce(Return(0));
  EXPECT_CALL(m_wrapper, close(7)).Times(1).WillOnce([](int) {
    errno = EIO;
    return -1;
  });
  EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia),
               exception::Errnum);
  m_drive.reset();
}

TEST_F(DriveMediaTest, ReadinessRefreshOpenFailureDoesNotDoubleClose) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce(Return(0));
  EXPECT_CALL(m_wrapper, open(_, _)).WillOnce([](const char*, int) {
    errno = EIO;
    return -1;
  });
  EXPECT_THROW(m_drive->waitUntilReady(std::chrono::steady_clock::now() + 1s, ReadinessWaitMode::AwaitMedia),
               exception::Errnum);
  EXPECT_EQ(1, m_closes);
  m_drive.reset();
  EXPECT_EQ(1, m_closes);
}

}  // namespace
}  // namespace cta::tape::drive
