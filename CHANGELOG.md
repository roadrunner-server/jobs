# Changelog

## [6.1.1](https://github.com/roadrunner-server/jobs/compare/v6.1.0...v6.1.1) (2026-11-27)


### Bug Fixes

* destroy worker pools during Stop without blocking on running handlers ([b35b384](https://github.com/roadrunner-server/jobs/commit/b35b384cebae1328f9119f266c10de5c2ff52a42))
* report service error when declared pipeline restart fails ([b468847](https://github.com/roadrunner-server/jobs/commit/b46884757c0b08f855c914ddd9435767cb17fc1c))
* restore pipeline running state on automatic restart ([1d42c1c](https://github.com/roadrunner-server/jobs/commit/1d42c1c86c0353eda59dfecc92b97defdcbcf463))
* retain unstarted jobs on worker allocation timeout ([0bf1d1c](https://github.com/roadrunner-server/jobs/commit/0bf1d1c7abaf9cd939c478bd4ddb1970d953f664))
* serialize concurrent pipeline declarations ([e6facc5](https://github.com/roadrunner-server/jobs/commit/e6facc5b759e74ff2580a24f6a2d526581bda407))

## [6.1.0](https://github.com/roadrunner-server/jobs/compare/v6.0.1...v6.1.0) (2026-11-25)


### Features

* support Redis pipelines in the Jobs configuration schema ([f59975a](https://github.com/roadrunner-server/jobs/commit/f59975aa1aa669dc1ab55279cc7db6b06711b4b4))

## Changelog

## Unreleased

- Accept Redis pipelines in the Jobs configuration schema. A driver can cancel an insert while the Jobs queue is full.
