# Installation instructions

Below you can find the instruction on where to find the publicly available RPMs for the CTA software and how to install these RPMs.

For the files installed by these packages and service behavior during installation, upgrade, and removal, see [RPM Packages and Services](rpm-packages-and-services.md).

## RPMs

**1. Setup the repo for *CTA-5*:**

Operators can choose between a *stable* or a *testing* channel for CTA:

- *stable*:
    - These releases have been deployed at CERN *in production* successfully.
- *testing*:
    - Updated more frequently, with more recent releases.
    - They are expected to work (CI testing was successful) but have not been deployed in production at CERN.

For *stable* CTA versions:

```shell
cat << EOF > /etc/yum.repos.d/public-stable-cta-5.repo
[public-stable-cta-5-alma9]
name=CERN Tape Archive
baseurl=https://cta-public-repo.web.cern.ch/stable/cta-5/el9/cta/x86_64
enabled=1
gpgcheck=0
EOF
```

For *testing* CTA versions:

```shell
cat << EOF > /etc/yum.repos.d/public-testing-cta-5.repo
[public-testing-cta-5-alma9]
name=CERN Tape Archive
baseurl=https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64
enabled=1
gpgcheck=0
EOF
```

**2. Search and/or install *cta-release*:**

The `cta-release` package will install the needed `.repo` files and the `cta-versionlock` tool (see next step).

```shell
dnf search cta-release --showduplicates
dnf install cta-release-<cta_version>.el9.x86_64
```

or, to get the latest version:

```shell
dnf install cta-release
```

**3. Configure *CTA/EOS* repos and dependencies:**

The `cta-versionlock` tool will update the versionlock file on the machine to lock CTA and all its dependencies to the right versions.

```shell
cta-versionlock apply
```

**4. Install additional repositories**

`epel-release` repository is needed before going further.

```
dnf install epel-release
```

**5. Install CTA and EOS:**

By default, CTA only comes with Postgres support for the catalogue. To enable Oracle support, you will have to explicitly install `cta-lib-catalogue-occi`. See section below.

Choose the appropriate packages, as needed.

```shell
dnf install cta-frontend # For the disk buffer + admin commands
dnf install cta-maintd # For executing various routines necessary for the correct working of CTA
dnf install cta-taped # For the drive. Must be on a server connected to tape drives
dnf install cta-rmcd # For the mediachanger. Must be on every server where a cta-taped process is running
# cta-release will also supply the required .repo files and versionlock entries for EOS
# Optional if using dCache
dnf install eos-server
dnf install eos-client
```

!!! info

    For how to configure and deploy CTA, see [Configurations](./configuration/example-configs.md) and [Recommendations](./recommendations.md)

### Note on Catalogue Database Support

CTA supports multiple database backends for the catalogue: Postgres and Oracle. Thanks to a plugin system, CTA only needs one of these two RPMs installed, depending on which backend is used:

- `cta-lib-catalogue-occi`
- `cta-lib-catalogue-postgres`

By default, installing any CTA package requiring `cta-lib-catalogue` will only install `cta-lib-catalogue-postgres`.

- That means: `dnf install cta-taped` will bring in `cta-lib-catalogue-postgres`, but **not** `cta-lib-catalogue-occi`

If you are using Oracle for the catalogue, you will have to explicitly install `cta-lib-catalogue-occi`

- In this case: `dnf install cta-lib-catalogue-occi cta-taped` will not bring in `cta-lib-catalogue-postgres`

In general, CTA will be happily installed as long as at least one of `cta-lib-catalogue-postgres` or `cta-lib-catalogue-occi` is present.
