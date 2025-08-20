# Remote Media Changer (RMC)

The Remote Media Changer takes care of all the tape library robotic control
functions. It is a venerable piece of software, going back to the days of SHIFT
at CERN (circa 1990). In its current iteration, it supports SCSI communication
with IBM and Spectra Logic tape libraries. The Oracle/StorageTek ACS (Automated
Cartridge System) interface was deprecated in 2021, after Oracle withdrew from
the tape market.

RMC consists of four software components:

mediachanger/          RMC client library for rmcd (linked by the tape server)
mediachanger/rmcd      Remote Media Changer Daemon
mediachanger/smc       SCSI Media Changer, client for rmcd (command-line tool)
mediachanger/librmc    Common library functions shared by rmcd and smc
