# Tape Libraries

Tape libraries are home to the tape media and tape drives, as well as everything needed in terms of robotics and communications to move the media between their home slot and a waiting drive.
It is the job of the library to keep track of which tape is where, and to respond to requests for the mounting and un-mounting of tapes.

## Physical libraries

Physical libraries vary from manufacturer to manufacturer in terms of terminology and functionality.
We recommend studying the manuals provided by your vendor.

At CERN we presently use IBM TS4500 and Spectra Logic TFinity libraries, so the vocabulary and concepts in these pages are influenced by their particulars.
Some terms to know include:

TODO

### Partitions

A tape library is partitioned into one or more sets of tape media and drives, which are called partitions.
Tapes are assigned to partitions through their corresponding VOLSER ranges, of which multiple may be assigned to one partition, without overlap between partitions.
Partitions act as hardware groupings which are independent from one another, and allow for features such as:

* Running CTA alongside other magnetic tape data management software products
* Moving tape media into special partitions without tape drives, such that they are safe from attempts at overwriting them (also known as *Safeguarded Tape*)

### Physical libraries and CTA

Within CTA one may administrate the physical libraries known to the system using the `cta-admin physicallibrary` command. 
Use of the physical library concept is optional, it us purely a convenience and monitoring/record-keeping feature.
For instance, it provides helpful features for operations, such as the disabling the associated parts of the infrastructure during maintenance windows, and monitoring of properties such as library slot usage.

#### Logical libraries

A physical library may be further partitioned into multiple *logical libraries* within CTA.
Each logical library has its own set of associated tape media, tape drives, etc.
This feature may for instance be used to organize multiple generations of media and drives within the same library.
A logical library must only contain compatible combinations of drives and media.


