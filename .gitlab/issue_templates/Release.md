<!-- Ensure the title of this ticket is the name of the release. E.g. `v5.X.Y.Z-R` -->

* See [Releasing a new version of CTA](https://cta.docs.cern.ch/latest/dev/ci/releases/)

## Release Requirements

<!-- Add any features/fixes that must be in the release here -->

## Additional Details

<!-- Should this release be used in production straight away? Normally, NO, it should be deployed and tested on PPS first. -->

<!-- Does this release require a schema upgrade? (Specify the schema version). -->

<!-- Add additional notes for deployment of this version, *e.g.* if new tables need to be populated. -->

<!-- Add additional notes if this version is part of a longer-term development/testing effort, *e.g.* tagged on a specific branch to fix an urgent production bug -->

## Release Checklist

- [ ] Stress test run
    - [ ] Stress test screenshot added
    - [ ] Stress test job linked
    - [ ] Stress test dashboard linked with timeframe
- [ ] Changelog updated
- [ ] Tag created
- [ ] RPMs present in `unstable` repository

Further actions such as adding RPMs to `testing` or `stable` can be done at a later point in time.
Once all steps have been completed, this issue can be closed.

## Stress Test

Screenshot of the stress test results:

Link to the CI `stress-test` job:

Link to the monitoring dashboard (with the correct timeframe):

<!-- If everything goes well for the stress test, create the Deployment ticket in the Operations repo. Otherwise, iterate in the comments to solve any problems. -->

/label ~type::release
