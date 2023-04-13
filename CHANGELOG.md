# Changelog

## 1.0.1
  * Updated Backoff Conditions added support for ConnectionResetError(104) and RemoteDisconnected Error.[#19](https://github.com/singer-io/tap-sailthru/pull/19)

## 1.0.0
  * Removed Additiona Package Dependencies [#16](https://github.com/singer-io/tap-sailthru/pull/16)
  * Support for parallel execution of integration tests [#15](https://github.com/singer-io/tap-sailthru/pull/15)
  * field datatype change for blast_query stream [14](https://github.com/singer-io/tap-sailthru/pull/14)
  * Fix for `blast_query` APi limit issue for records > 540 Days[14](https://github.com/singer-io/tap-sailthru/pull/14)
  * Fix `failure` Bug on empty api response [14](https://github.com/singer-io/tap-sailthru/pull/14)
  * Primary key  Uniqueness for purchase_log stream [14](https://github.com/singer-io/tap-sailthru/pull/14)

## 0.2.0
  * Refactor code to class based [#5](https://github.com/singer-io/tap-sailthru/pull/5)
  * Implement TimeOut Request [#6](https://github.com/singer-io/tap-sailthru/pull/6)
  * Verify Credentials in Discover Mode [#7](https://github.com/singer-io/tap-sailthru/pull/7)
  * Refactor Error Handling [#8](https://github.com/singer-io/tap-sailthru/pull/8)
  * Fix date_windowing for purchase_log stream [#9](https://github.com/singer-io/tap-sailthru/pull/9)
  * Primary key  Uniqueness for purchase_log stream [#10](https://github.com/singer-io/tap-sailthru/pull/10)
  * Updated Documentation
  * Official Beta Release

