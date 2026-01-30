# Changelog

## [0.3.1](https://github.com/cccteam/db-initiator/compare/v0.3.0...v0.3.1) (2026-01-30)


### Code Upgrade

* go 1.25.5 =&gt; 1.25.6 ([#118](https://github.com/cccteam/db-initiator/issues/118)) ([58544e8](https://github.com/cccteam/db-initiator/commit/58544e80c357d5e75b0e70114582cec7274b9091))

## [0.3.0](https://github.com/cccteam/db-initiator/compare/v0.2.17...v0.3.0) (2026-01-28)


### ⚠ BREAKING CHANGES

* Changes to interfaces for consistency

### Features

* Enhance migrations ([#115](https://github.com/cccteam/db-initiator/issues/115)) ([c231352](https://github.com/cccteam/db-initiator/commit/c2313527c5e09e465486b1a9976c0bdd653ea7b6))
* Enhance Migrator for spanner to enable dropping and data migrations ([c231352](https://github.com/cccteam/db-initiator/commit/c2313527c5e09e465486b1a9976c0bdd653ea7b6))


### Documentation

* add more descriptive readme ([c231352](https://github.com/cccteam/db-initiator/commit/c2313527c5e09e465486b1a9976c0bdd653ea7b6))


### Code Refactoring

* Changes to interfaces for consistency ([c231352](https://github.com/cccteam/db-initiator/commit/c2313527c5e09e465486b1a9976c0bdd653ea7b6))

## [0.2.17](https://github.com/cccteam/db-initiator/compare/v0.2.16...v0.2.17) (2025-12-03)


### Code Upgrade

* go 1.25.3 =&gt; 1.25.5 ([#111](https://github.com/cccteam/db-initiator/issues/111)) ([d0a3288](https://github.com/cccteam/db-initiator/commit/d0a32883a7d34249344c7d3372cdccd0f59e8b3b))

## [0.2.16](https://github.com/cccteam/db-initiator/compare/v0.2.15...v0.2.16) (2025-12-03)


### Code Upgrade

* crypto v0.43.0 =&gt; v0.45.0 ([#108](https://github.com/cccteam/db-initiator/issues/108)) ([3d171f7](https://github.com/cccteam/db-initiator/commit/3d171f7ef6278d856ed4130adf1573de39970652))

## [0.2.15](https://github.com/cccteam/db-initiator/compare/v0.2.14...v0.2.15) (2025-11-27)


### Bug Fixes

* Disable logging for PostgreSQL test migrations as it is too noisy ([#103](https://github.com/cccteam/db-initiator/issues/103)) ([369173b](https://github.com/cccteam/db-initiator/commit/369173bfae60e21d39a43678b36d5786d3a15ef3))
* Fix release please ([#105](https://github.com/cccteam/db-initiator/issues/105)) ([9e70f62](https://github.com/cccteam/db-initiator/commit/9e70f62c8f6a4bf101cdf7474933b62b43993f4e))

## [0.2.14](https://github.com/cccteam/db-initiator/compare/v0.2.13...v0.2.14) (2025-11-14)


### Bug Fixes

* Close client when calling close on SpannerMigrationService ([#100](https://github.com/cccteam/db-initiator/issues/100)) ([fc92ebd](https://github.com/cccteam/db-initiator/commit/fc92ebd917d130c1a2393716f1c397f6dbb4346a))


### Code Cleanup

* Enable metrics for SpannerMigrationService ([#100](https://github.com/cccteam/db-initiator/issues/100)) ([fc92ebd](https://github.com/cccteam/db-initiator/commit/fc92ebd917d130c1a2393716f1c397f6dbb4346a))

## [0.2.13](https://github.com/cccteam/db-initiator/compare/v0.2.12...v0.2.13) (2025-11-03)


### Bug Fixes

* Revert "upgrade: replace docker v28.3.3+incompatible =&gt; moby v28.3.3+incompatible ([#87](https://github.com/cccteam/db-initiator/issues/87))" ([#92](https://github.com/cccteam/db-initiator/issues/92)) ([e33c3e2](https://github.com/cccteam/db-initiator/commit/e33c3e2e36fa9579eb2a18e24704f79a31a9a8ba))


### Code Upgrade

* go 1.25.3 and deps ([#98](https://github.com/cccteam/db-initiator/issues/98)) ([adf0609](https://github.com/cccteam/db-initiator/commit/adf0609afb4a03b3c453116dd01ef47d262abee8))

## [0.2.12](https://github.com/cccteam/db-initiator/compare/v0.2.11...v0.2.12) (2025-08-14)


### Bug Fixes

* Add token so workflows will trigger ([#91](https://github.com/cccteam/db-initiator/issues/91)) ([f6e4bb6](https://github.com/cccteam/db-initiator/commit/f6e4bb679ee8e7e246e3e1e3f037bfe58ae63253))


### Code Refactoring

* Ignore Release Please branches ([#90](https://github.com/cccteam/db-initiator/issues/90)) ([201022e](https://github.com/cccteam/db-initiator/commit/201022ee198bf9f2d83ca89d185ab602a3f3fdc3))
* Switch from the Release Please Bot to Action ([#88](https://github.com/cccteam/db-initiator/issues/88)) ([94e2ca8](https://github.com/cccteam/db-initiator/commit/94e2ca8612fe265ff90801fbaba5be67ca6f0cc5))


### Code Upgrade

* replace docker v28.3.3+incompatible =&gt; moby v28.3.3+incompatible ([#87](https://github.com/cccteam/db-initiator/issues/87)) ([b970a4b](https://github.com/cccteam/db-initiator/commit/b970a4bf342d8b9f5ab562cb50f022cfa1501bd2))

## [0.2.11](https://github.com/cccteam/db-initiator/compare/v0.2.10...v0.2.11) (2025-08-11)


### Code Upgrade

* docker v28.2.2 =&gt; v28.3.3 ([#82](https://github.com/cccteam/db-initiator/issues/82)) ([6f27b5b](https://github.com/cccteam/db-initiator/commit/6f27b5b001624039dfa44827a9bae2940dab2b8c))
* go =&gt; 1.24.6 ([#85](https://github.com/cccteam/db-initiator/issues/85)) ([c582722](https://github.com/cccteam/db-initiator/commit/c5827223a30f83e3e07d0ddced5a64a62686a5a7))

## [0.2.10](https://github.com/cccteam/db-initiator/compare/v0.2.9...v0.2.10) (2025-07-23)


### Features

* Add logging for migration progress ([#77](https://github.com/cccteam/db-initiator/issues/77)) ([7e72d57](https://github.com/cccteam/db-initiator/commit/7e72d5719bc0b16e66daa13f961cd028ddaf7f47))

## [0.2.9](https://github.com/cccteam/db-initiator/compare/v0.2.8...v0.2.9) (2025-06-17)


### Features

* Add support to connect to existing database to run migrations ([#73](https://github.com/cccteam/db-initiator/issues/73)) ([e426b72](https://github.com/cccteam/db-initiator/commit/e426b725afdbf5e9fce22f6940f7a2fbd90d6811))

## [0.2.8](https://github.com/cccteam/db-initiator/compare/v0.2.7...v0.2.8) (2025-06-12)


### Code Upgrade

* go 1.24.2 to 1.24.4 ([#70](https://github.com/cccteam/db-initiator/issues/70)) ([0a27cea](https://github.com/cccteam/db-initiator/commit/0a27cea86367ca7a6bcd8560eb92cd1a1d730c1a))

## [0.2.7](https://github.com/cccteam/db-initiator/compare/v0.2.6...v0.2.7) (2025-04-30)


### Code Upgrade

* `GO` from `1.23.6` to `1.24.2` ([#65](https://github.com/cccteam/db-initiator/issues/65)) ([ae3e538](https://github.com/cccteam/db-initiator/commit/ae3e538af8bbfe49247e718f4873d5f8e61bddf2))

## [0.2.6](https://github.com/cccteam/db-initiator/compare/v0.2.5...v0.2.6) (2025-02-12)


### Code Upgrade

* go dependencies ([#59](https://github.com/cccteam/db-initiator/issues/59)) ([cd3b414](https://github.com/cccteam/db-initiator/commit/cd3b41428796b27afd238b3e83ffcfd71c21ed5b))

## [0.2.5](https://github.com/cccteam/db-initiator/compare/v0.2.4...v0.2.5) (2025-01-31)


### Dependencies

* Upgrade x/net to 0.33.0 ([#54](https://github.com/cccteam/db-initiator/issues/54)) ([b8639b8](https://github.com/cccteam/db-initiator/commit/b8639b8e506832c5484cdd849e732fb912d20dc9))


### Code Upgrade

* Go version from `1.23.4` to `1.23.5` ([#56](https://github.com/cccteam/db-initiator/issues/56)) ([ab09447](https://github.com/cccteam/db-initiator/commit/ab0944785c3b012a19e555558df261942530ff01))
* Upgraded to go 1.23.4 and upgraded packages to latest. ([#52](https://github.com/cccteam/db-initiator/issues/52)) ([80affcf](https://github.com/cccteam/db-initiator/commit/80affcf24b5fc945e21f17d36d203e5ee85dfe62))

## [0.2.4](https://github.com/cccteam/db-initiator/compare/v0.2.3...v0.2.4) (2024-12-05)


### Features

* Upgrade mograte fork ([#48](https://github.com/cccteam/db-initiator/issues/48)) ([cacf36f](https://github.com/cccteam/db-initiator/commit/cacf36f663b4a0deba9cba03944d00f242eb85c7))

## [0.2.3](https://github.com/cccteam/db-initiator/compare/v0.2.2...v0.2.3) (2024-11-02)


### Bug Fixes

* Fix breaking change in spanner package when connecting to emulator ([#44](https://github.com/cccteam/db-initiator/issues/44)) ([7b86084](https://github.com/cccteam/db-initiator/commit/7b860844b33699ecaea413aa86af249015e37167))

## [0.2.2](https://github.com/cccteam/db-initiator/compare/v0.2.1...v0.2.2) (2024-11-01)


### Features

* Add passthrough protocol to emulator endpoint ([#42](https://github.com/cccteam/db-initiator/issues/42)) ([f818646](https://github.com/cccteam/db-initiator/commit/f818646cbf5447f7c78c53258ba218f7c43c954c))

## [0.2.1](https://github.com/cccteam/db-initiator/compare/v0.2.0...v0.2.1) (2024-09-19)


### Dependencies

* Update Dependencies ([#35](https://github.com/cccteam/db-initiator/issues/35)) ([317628b](https://github.com/cccteam/db-initiator/commit/317628bf467fa5a108e66b3a86085d83b35baf9f))

## [0.2.0](https://github.com/cccteam/db-initiator/compare/v0.1.2...v0.2.0) (2024-08-29)


### ⚠ BREAKING CHANGES

* Changed the method name on `SpannerContainer` from `CreateTestDatabase()` to `CreateDatabase()`

### Code Refactoring

* Changed the method name on `SpannerContainer` from `CreateTestDatabase()` to `CreateDatabase()` ([16404cd](https://github.com/cccteam/db-initiator/commit/16404cd4d40ab1c1fe4ecd757bba7c278e64627e))
* Rename SpannerContainer methods to be consistent with PostgresContainer (32) ([16404cd](https://github.com/cccteam/db-initiator/commit/16404cd4d40ab1c1fe4ecd757bba7c278e64627e))

## [0.1.2](https://github.com/cccteam/db-initiator/compare/v0.1.1...v0.1.2) (2024-08-25)


### Features

* Added support for PostgreSQL. ([#9](https://github.com/cccteam/db-initiator/issues/9)) ([013fa74](https://github.com/cccteam/db-initiator/commit/013fa7434295b939352a75c8e78b314e55059625))

## [0.1.1](https://github.com/cccteam/db-initiator/compare/v0.1.0...v0.1.1) (2024-07-03)


### Code Upgrade

* Update Go version to 1.22.5 to address GO-2024-2963 ([#23](https://github.com/cccteam/db-initiator/issues/23)) ([14b2b44](https://github.com/cccteam/db-initiator/commit/14b2b442f1083c9e2a1aa32148943e60d2a84ae6))
* Update workflows to latest version ([#18](https://github.com/cccteam/db-initiator/issues/18)) ([9916ab4](https://github.com/cccteam/db-initiator/commit/9916ab4f7c3e028b1574a208fba0ad8c3bd23d1f))

## 0.1.0 (2024-06-18)


### Features

* Allow the spanner container version to be configured ([#13](https://github.com/cccteam/db-initiator/issues/13)) ([241954a](https://github.com/cccteam/db-initiator/commit/241954ac932ff9d9f2f1b755ac7c858ad3a15cbb))
* Spanner implementation ([#1](https://github.com/cccteam/db-initiator/issues/1)) ([a437249](https://github.com/cccteam/db-initiator/commit/a437249ffd7ee66259e538e2801f16e45a288a1c))


### Bug Fixes

* Align the repo and module naming ([#12](https://github.com/cccteam/db-initiator/issues/12)) ([e8589e6](https://github.com/cccteam/db-initiator/commit/e8589e6f5fcd3543368cd9236468962a25ff07fe))


### Code Refactoring

* Add CI/CD config ([#2](https://github.com/cccteam/db-initiator/issues/2)) ([b8d62c5](https://github.com/cccteam/db-initiator/commit/b8d62c5cc29e79a08c95f4b598f4ed1273a78b87))
* Rename public functions, consts, and structs ([#4](https://github.com/cccteam/db-initiator/issues/4)) ([dbb8bdf](https://github.com/cccteam/db-initiator/commit/dbb8bdf2810138564133fb467a859bf3335b20d8))
