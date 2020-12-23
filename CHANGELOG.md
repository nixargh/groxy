# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [0.4.1] - 2020-12-21
### Fixed
- Wrong **TransformQueue--** action.

## [0.4.0] - 2020-12-20
### Changed
- Create multiple connections for Sender and send a number of packs to each before close.

### Added
- Stats server that returns current state by HTTP.
- Re-send of metrics after send failure.

### Fixed
- Sending to closed connections.

## [0.3.0] - 2020-12-16
### Fixed
- Send connection TLS version pinned on *1.2* which fixed a **CD** termination error on Haproxy side.

## [0.2.0] - 2020-12-13
### Added
- Connection options.

### Fixed
- CPU overload.

## [0.1.1] - 2020-12-04
### Added
- `./.github/workflows/go.yml` CI that creates releases and builds an executable for it.

## [0.1.0] - 2020-12-04
### Added
- `main.go` first working version.