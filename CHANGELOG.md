# Changelog

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
