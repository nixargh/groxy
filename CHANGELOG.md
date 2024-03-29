# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2022-10-04
### Fixed
- `main.go` Exit code after showing a version.

### Changed
- `main.go` Catch both *SIGINT* and *SIGTERM* and exit with **0** code.

## [3.0.0] - 2022-09-30
### Changed
- **-graphiteAddress** takes `hostname:port` pair.
- Multiple senders allowed by repeating **-graphiteAddress** flag.

### Removed
- **-graphitePort** flag.

## [2.2.0] - 2022-06-07
### Changed
- Init as go module.

### Removed
- `receiver.go` 10 minutes deadline for incoming connections.

### Added
- `main.go` **-version** flag to show **Groxy** version.

## [2.1.0] - 2022-01-30
### Added
- Mutual TLS support.
- New parameter **-forceTenant** that allows to rewrite metric tenant if it is already set.

## [2.0.1] - 2022-01-08
### Fixed
- `state` do not add dot to empty prefix at own metrics.

### Removed
- **-TLS** argument in favor of **-tlsOutput** and **-tlsInput**.

### Added
- Add compression for both `receiver` and `sender`.
- New arguments: **-tlsOutput**, **-tlsInput**, **-tlsInputCert**, **-tlsInputKey**.
- `state` new metrics: **OutBytes**, **OutBpm**.

## [1.3.0] - 2021-06-03
### Added
- **hostname** argument. Allows setting hostname instead of getting a real one (good for Docker).

## [1.2.1] - 2021-02-28
### Fxied
- Wait time when no metrics are in the queue from **10** to **5** milliseconds.

## [1.2.0] - 2021-02-28
### Changed
- The size of a pack from **1000** to **10000**. Default packs limit adjusted as well.

## [1.1.2] - 2021-02-25
### Fixed
- Bump version.

## [1.1.1] - 2021-02-25
### Fixed
- Do not die if *runSender* can't connect to Graphite server.

## [1.1.0] - 2021-02-18
### Changed
- Do not rewrite **tenant** if it is set.

### Added
- **systemTenant** and **systemPrefix** arguments to allow sending groxy metrix to some tenant and path different from main **tenant** and **path**.

## [1.0.0] - 2021-02-17
### Added
- **limitPerSec** new argument to limit the number of metric packs sent per second. Default is 10 packs that is equals to 10x1000=10000 metrics per second or 600000 mpm.
- New metric **PacksOverflewError** exported as **packs_overflew_error**.

### Changed
- Send pointers to *Metric* through chanels instead of *Metric* itself. Reduce memory consumption (~6-10 times).

## [0.9.0] - 2021-02-09
### Changed
- Replace **uint64** with **int64** for state counters.
- Refactor **sendMetric**.

### Fixed
- [#1](https://github.com/nixargh/groxy/issues/1) use **sync/atomic** for counters.

## [0.8.1] - 2021-01-28
### Changed
- Replace **int64** with **uint64** for state counters.

## [0.8.0]
### Added
- **queue** stat instead of **out_queue** because **out_queue** now shows diff between outputed and transformed metrics.
- Send **groxy** state as metrics to Graphite.

## [0.7.0] - 2021-01-24
### Changed
- Logging with **logrus** library.
- Both **out_queue** and **transform_queue** are not counters but calculated once per second.

## [0.6.1] - 2021-01-17
### Added
- Groxy **version** to **State**.

### Fixed
- **sendMetric** changes.

## [0.6.0] - 2021-01-11
### Changed
- **-immutablePrefix** can be set many times and used as a slice.

## [0.5.0] - 2020-12-28
### Changed
- Use a single out connection and pass packs as pointers to array.

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
