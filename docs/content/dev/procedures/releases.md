# Releasing a new version of CTA

This page covers the process of creating a new CTA release. New versions of CTA are released periodically, which involves publishing the binaries to one or more of these repositories:

- **unstable**: <https://cta-public-repo.web.cern.ch/unstable/cta-5/el9/cta/x86_64/>
    - These versions have passed CI and have been stress-tested, but have not been deployed in production.
- **testing**: <https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64/>
    - These versions have passed CI and have (typically) been stress-tested. May contain versions that are not meant to be deployed in production.
- **stable**: <https://cta-public-repo.web.cern.ch/stable/cta-5/el9/cta/x86_64/>
    - These versions of CTA have been tested and deployed in production at CERN.

We aim to release an unstable version of CTA every 2-4 weeks. Stable releases happen more rarely as these require deployment and testing in production here at CERN.
If required, new releases can be created for testing purposes. These can be created on an ad-hoc basis and must clearly be identified as `testing`. They must be removed once no longer needed.

## 1. Choose a version number for the new CTA release

CTA versions are in the format:

```txt
<xrootd>.<major>.<minor>.<patch>-<package>
```

Example:

```txt
5.10.11.0-1
```

Where

- `<xrootd>`:
    - XrootD version.
    - Currently only `5` is allowed.
- `<major>`:
    - Major number, bumped up every time a new catalogue is released.
    - All releases with a certain `<major>` value depend on the same CTA Catalogue schema version.
- `<minor>`:
    - Minor number, bumped up for every release that does not require a new Catalogue version.
    - Backward compatible, regarding Catalogue schema.
    - Does not guarantee to be backward compatible with other systems (i.e. EOS). Dependency software versions, such as EOS, should follow the versioning defined in `versionlock.list`.
- `<patch>`:
    - Patch number, bumped when applying a fix to an old version (including public releases).
    - Should not include new features, only fixes.
- `<package>`:
    - Package number, for any changes in the packaging/built that do require the code logic to be modified.

All DB schema releases should bump up the `<major>` version number (*eg.:* `4.10.0.0-1`).
Non-BD schema releases (just code updated) should bump up only the `<minor>` version number (*eg.:* `4.10.1.0-1`).
*Optionally*, we can append a `<suffix>` to the CTA version:

```txt
<xrootd>.<major>.<minor>.<patch>-<package>.<suffix>
```

Any value `[a-zA-Z0-9]+` is accepted for `<suffix>`. It can be used to include extra information in the tag, without affecting how the CI works.
Some examples/conventions:

- `5.10.11.0-1.el9` # Alma 9 release, only needed until CentOS-7 reaches EOL
- `5.10.11.0-1.rc1` # Release canditate, for tagging test releases that will never be used in production.
- `5.10.11.0-1.test1` # Used for testing the tagging procedure itself

### 1.1 Special case: Catalogue release

As mentioned above, all DB schema releases should bump up the `<major>` version.

**These releases should contain no code changes, unless this can't be avoided.**

They can only contain the following changes:

- Changing the [`cta-catalogue-schema` submodule](https://gitlab.cern.ch/cta/CTA/-/tree/main/catalogue) commit, in order to reference a new **tagged** CTA Catalogue release.
    - It's important that the CTA Catalogue version is tagged, and includes all the migration scrips.
- Updating the supported Catalogue versions in [`project.json` (click for link)](https://gitlab.cern.ch/cta/CTA/-/blob/main/project.json).
    - A CTA catalogue release, must be compatible with both the old and the new catalogue version.
    - Example:
        - If `5.11.0.0-1` introduces the new CTA Catalogue version `15.0` (updated from `14.0`) then it must be compatible with both `14.0` and `15.0` versions, to allow for the upgrade to go smoothly.

## 2. Create an issue on GitLab with the label "type::release"

All  CTA releases must be tracked by a dev issue with the label `type::release` [(click to list current releases)](https://gitlab.cern.ch/cta/CTA/-/issues/?sort=updated_desc&state=all&label_name%5B%5D=type%3A%3Arelease).
The CTA project already provides a [template for `Release` issues (click for link)](https://gitlab.cern.ch/cta/CTA/-/issues/new?issuable_template=Release).

## 3. Changelog Update

On the branch where the tag will be generated (typically from `main`), the developer can trigger two manual pipelines:

- `changelog-preview`
- `changelog-update`

The first job generates a preview of which commits will be included in the changelog. The developer HAS to specify the `RELEASE_VERSION` variable in the ci. Otherwise, it will not know which version to generate a changelog for, since nothing new has been tagged yet. This version should not start with a `v`.

!!! warning
    The maximum number of commits that can be previewed for the changelog is 100. If this is exceeded, a warning will be produced.

There are three categories here:

- `Error`: these commits will not end up in the changelog
- `Warning`: these commits will end up in the changelog, but perhaps not as expected
- `OK`: these commits will end up in the changelog as expected.

This `changelog-preview` is just a sanity check; the developer will get the opportunity to fix any errors manually in the `changelog-update` job.
In most cases, the developer can then trigger the `changelog-update` job. This will do a number of things:

- Creates a new branch called `<RELEASE_VERSION>-changelog-update` based on the `SOURCE_BRANCH` (will be the `main` branch by default)
- Updates the `CHANGELOG.md` as shown in the output of the previous job
- Adds an entry in the changelog of `cta.spec.in`
- Pushes these updates to the `<RELEASE_VERSION>-changelog-update` branch
- Creates a Merge Request from the branch into `SOURCE_BRANCH`
    - Sets the title:  `Changelog update for <RELEASE_VERSION>`
    - Puts the output of the changelog preview as the description
    - Assigns the person who triggered the pipeline as a reviewer for the merge request
    - Gives it the `~type::release` label
    - Adds a review comment to the changelog entry of `cta.spec.in` indicating that more details

The developer should then go through the changes in the MR and verify that the `CHANGELOG.md` contains all the features, bug fixes and improvements for this release. Each one of the entries in the *Release Notes* file should contain a link to an issue in the CTA project.

In case of doubt about any commit, contact the creator of said commit directly. Once everything has been checked (and possible updated), the MR can be merged by the developer.

## 4. Run all important system tests

Check that all important system tests run successfully in the CI. Besides all the mandatory tests, some of the optional ones can also be useful (if relevant):

- `test-liquibase-update`: Important only for CTA Catalogue releases. Checks that the migration scripts from catalogue version `X` to `Y` result into a correct version `Y` schema.
- `test-external-tape-formats`: Performs a test with the OSM tape format.

## 5. Perform a stress test

The release candidate should be stress tested. This can be done by running the `stress-test` job in CI on the `main` branch for the commit you want to stress test.

Results from the stress test should be added to the release issue by the person performing the test. Any issues arising from the stress test should be addressed.

## 6. Create a Tag on GitLab

Once all the above is complete, the new release can be [tagged in Gitlab](https://gitlab.cern.ch/cta/CTA/-/tags/new). Tagging a release will trigger a *"Release"* pipeline, which will automatically build the RPMs for the new CTA release. Optionally, it also allows some system tests to be done.

## 7. Publish the RPMs

After the CI is complete, the developer can trigger these manual jobs:

- `internal-release-cta`:
    - Publishes the RPMs to the CTA team internal repo. These become available for deployments at CERN.
- `public-release-cta-unstable`:
    - Publishes the RPMs to the [CTA public unstable repo (external)](https://cta-public-repo.web.cern.ch/unstable/).
- `public-release-cta-testing`:
    - Publishes the RPMs to the [CTA public testing repo (external)](https://cta-public-repo.web.cern.ch/testing/).
- `public-release-cta-stable`:
    - Publishes the RPMs to the [CTA public stable repo (external)](https://cta-public-repo.web.cern.ch/stable/).

!!! warning
    The public release step should be taken only after the internal release has been in use in production for some time, in order to guarantee that it's stable and that no buggy RPMs are released to the public. If needed, a [testing release](https://cta-public-repo.web.cern.ch/testing/) can be done to make the new RPMs immediately available to the public.

## 8. Announce any public releases

Any stable public release should be registered on the CTA gitlab project [`Releases` tab](https://gitlab.cern.ch/cta/CTA/-/releases), and announced on the [CTA Community forum](https://cta-community.web.cern.ch/).
