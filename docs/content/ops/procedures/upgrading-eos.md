# Upgrading EOS *tape* instance

## CTA specific guidelines

### Mix and match tested versions

!!! danger
    CTA software is only tested in CTA Continuous Integration against a few specific EOS versions. CTA workflow can break if mixing any EOS software against any CTA version.

Once the CTA version has been chosen, please refer to the CTA provided versionlock file to understand which EOS software should be deployed along with the target CTA version.

For example: for a specific CTA version install `cta-release` at that version to generate the *yum versionlock file* that indicates the required EOS version, or look in the corresponding tag: for example [CTA 5.11.18.0-1 project.json file](https://gitlab.cern.ch/cta/CTA/-/blob/v5.11.18.0-1/project.json) to find the version values.

!!! tip
    Rollout a specific EOS version to all your EOS tape buffer instances starting with a test instance, followed by less critical EOS tape buffer instances (repack, backup,...).
    Managing a limited set of EOS software version allows to limit the complexity of the EOS CTA service operation.

### Stop corresponding tape activity

CTA tape backend and EOS are working together via synchronous protobuf messages.

During the EOS upgrade procedure the EOS namespace server will be unavailable during the upgrade.
**In theory upgrading EOS instance should not take more than 5 minutes but in practice it can quickly go wrong and require much more time.**

This is likely to affect the critical EOS <-> CTA synchronous communication workflow especially when the EOS MGM is upgraded:

- Archiving files stuck in EOS buffer:
  - `cta-taped` cannot read EOS source file resulting in failed transfer sessions and dismounted tapes on CTA side
  - `cta-maintd` cannot report archived files resulting in files stuck in EOS buffer and written on tape (dark data)
- Retrieving files that cannot be retrieved by end user anymore:
  - `cta-taped` cannot write recalled files to EOS buffer and cannot report staging failure or cleanup EOS attributes to EOS

For all these reasons it is **strongly advised to stop all tape activity tied the the upgrading EOS instance**.

#### Identify and stop affected VO tape activity

1. Identify CTA Virtual Organizations *VO*s affected by the CTA diskinstance upgrade running `cta-admin vo ls`
2. Prevent any new tape activity for the affected VOs by setting their `read-max-drives` and `write-max-drives` at 0
3. Wait for all tapes mounted for these VOs to be ejected (or kill current tape sessions) before starting EOS instance upgrade

!!! warning
    Do not forget to write down all vo *read-max-drives* and *write-max-drives* values as you will have to configure these back after the EOS upgrade

## Upgrading EOS software

Upgrading EOS software stack is outside of the scope of this documentation.
Indeed upgrading EOS software can be tricky when breaking changes are introduced.

For example:

- *in_memory* namespace to *quarkdb* namespace
- Removal of `eos@MQ` service requires a full reconfiguration and stopping all EOS services (namespace servers and disk servers) before starting the instance from scratch
- ...

Upgrading EOS is best documented on the [EOS documentation site](https://eos-docs.web.cern.ch/). Additional information can be found on [EOS community website](https://eos-community.web.cern.ch/).

## Resume CTA Tape activity

Reconfigure affected VOs with their previous `read-max-drives` and `write-max-drives` values.
