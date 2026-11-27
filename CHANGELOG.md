# Changelog

## [6.1.1](https://github.com/roadrunner-server/jobs/compare/v6.1.0...v6.1.1) (2026-11-27)

### 🐛 Bug Fixes

- **Concurrent pipeline declarations**: Serialize concurrent `Declare` calls for the same pipeline name so one declaration cannot replace or orphan another running driver. See [BUG #2438](https://github.com/roadrunner-server/roadrunner/issues/2438) and [PR #164](https://github.com/roadrunner-server/jobs/pull/164).
- **Automatic restart of declared pipelines**: Restore declared pipelines to their prior running state after a worker pool automatic restart. See [BUG #2440](https://github.com/roadrunner-server/roadrunner/issues/2440) and [PR #166](https://github.com/roadrunner-server/jobs/pull/166).
- **Declared pipeline restart error reporting**: Send an error to the plugin error channel when restarting a declared pipeline fails after a worker pool automatic restart. See [BUG #2439](https://github.com/roadrunner-server/roadrunner/issues/2439) and [PR #167](https://github.com/roadrunner-server/jobs/pull/167).
- **Graceful shutdown with running handlers**: Destroy worker pools during `Stop` without blocking on running handlers, so shutdown finishes within the grace period. See [BUG #2443](https://github.com/roadrunner-server/roadrunner/issues/2443) and [PR #168](https://github.com/roadrunner-server/jobs/pull/168).
- **Unstarted jobs on worker allocation timeout**: Retain unstarted jobs when worker allocation times out without acknowledging them. See [BUG #2447](https://github.com/roadrunner-server/roadrunner/issues/2447) and [PR #169](https://github.com/roadrunner-server/jobs/pull/169).

## [6.1.0](https://github.com/roadrunner-server/jobs/compare/v6.0.1...v6.1.0) (2026-11-25)


### Features

* support Redis pipelines in the Jobs configuration schema ([f59975a](https://github.com/roadrunner-server/jobs/commit/f59975aa1aa669dc1ab55279cc7db6b06711b4b4))

## Changelog

## Unreleased

- Accept Redis pipelines in the Jobs configuration schema. A driver can cancel an insert while the Jobs queue is full.
