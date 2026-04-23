# PR: Startup Stability, Logging Overhead Reduction, and Build Pipeline Cleanup

## Summary
This PR improves consolidator startup stability and observability by addressing SQLite contention handling, reducing logging overhead, tightening device reload/count behavior, and simplifying build-time packaging/report generation.

## Problem Statement
During startup/reload at scale, we observed:
- Intermittent SQLite lock contention symptoms in stats paths.
- Thread stalls dominated by synchronous logging overhead from very high debug volume.
- Counter timing mismatch where detected devices could lag while registered/initialized advanced.
- Slow build flow due to always-on third-party license retrieval.
- Maven install instability when assembly output directory was treated as an attachable artifact.

## Scope of Changes

### 1) SQLite stats hardening
Files:
- root/core/src/main/java/com/synclite/consolidator/watchdog/Monitor.java
- root/core/src/main/java/com/synclite/consolidator/processor/DeviceStatsCollector.java

Changes:
- Applied SQLite pragmas on relevant connections:
  - `PRAGMA busy_timeout = 5000`
  - `PRAGMA journal_mode = WAL`
  - `PRAGMA synchronous = NORMAL`
- Added safer transaction cleanup in monitor stats dumper:
  - rollback on failure when autocommit had been disabled
  - explicit autocommit reset after rollback
- Added explicit autocommit reset to true after commits in stats collector write paths.
- Added query timeout for stats-read path where device upload roots are reconstructed.

Impact:
- Better resilience under lock pressure.
- Lower chance of lingering dirty transaction state on error.

### 2) Device reload/order and count consistency
Files:
- root/core/src/main/java/com/synclite/consolidator/device/DeviceLocator.java
- root/core/src/main/java/com/synclite/consolidator/SyncDriver.java
- root/core/src/main/java/com/synclite/consolidator/device/Device.java

Changes:
- Device reload now prefers stage scan first; stats-based reload is fallback when stage returns empty.
- Added startup/reload instrumentation logs for detector, locator, and queueing behavior.
- Added `refreshMonitorDeviceCounts()` in `SyncDriver` to compute/update detected/failed/registered/initialized together from current state.
- Hooked count refresh into relevant scheduler/reload paths.
- Updated `DeviceLocator.buildDevices()` to set detected count incrementally as devices are added.
- Fixed `Device` instance map behavior:
  - normalize cache lookup by upload path
  - safer put-if-absent flow
  - improved remove/find behavior for upload-root keyed instances.

Impact:
- Startup path less prone to stalls from stats DB dependence.
- Device counters converge more predictably and are refreshed consistently.
- Better visibility when debugging detector and locator behavior.

### 3) Logging overhead reduction
Files:
- root/core/src/main/resources/log4j.properties
- root/core/src/main/java/com/synclite/consolidator/watchdog/Monitor.java
- root/core/src/main/java/com/synclite/consolidator/device/DeviceLocator.java
- root/core/src/main/java/com/synclite/consolidator/SyncDriver.java

Changes:
- Default root level moved to INFO.
- Removed duplicate file appender writing to same trace target.
- Simplified pattern from caller-location-heavy formatting to class-based context.
- Reduced row-level stats logging by sampling in stats reload flow.
- Added structured startup/reload debug statements for targeted diagnostics.

Impact:
- Lower synchronous log I/O pressure.
- Cleaner operational logs with less startup noise at default level.

### 4) Log framework dependency stack simplification
Files:
- root/core/pom.xml
- root/web/pom.xml

Changes:
- Replaced Log4j2 bridge stack with reload4j + SLF4J reload4j binding:
  - removed log4j-1.2-api/log4j-api/log4j-core/log4j-slf4j2-impl
  - added `ch.qos.reload4j:reload4j:1.2.25`
  - added `org.slf4j:slf4j-reload4j:2.0.17`

Impact:
- Simpler logging chain for log4j-1.x API usage.
- Reduced bridge/adapter complexity.

### 5) Build pipeline reliability + speed
Files:
- root/core/pom.xml

Changes:
- Prevented assembly directory from being attached as install artifact:
  - `<attach>false</attach>` in maven-assembly-plugin configuration.
- Moved `license-maven-plugin` execution into opt-in profile `license-report`.

Impact:
- `mvn install` no longer fails due to directory attachment issue.
- Normal build avoids expensive license retrieval calls.

## Additional behavior fix
File:
- root/core/src/main/java/com/synclite/consolidator/processor/DeviceLogCleaner.java

Changes:
- Switched cleanup to best-effort contiguous progression with warning logs on failures.
- Keeps `cleanedUpto` moving only across contiguous successfully cleaned segments.

Impact:
- More robust cleanup behavior under transient failures.

## Validation Performed
Build/compile checks run:
- `mvn -pl core -DskipTests compile`
- `mvn -Drevision=oss clean install -DskipTests`
- `mvn -Drevision=oss -DskipTests package`

Runtime inspection:
- Consolidator trace shows startup completion and stable monitor counts over cycles.
- No recent `SQLITE_BUSY`/exception pattern in inspected tail during latest run.

## Risk Assessment
- Moderate: touches startup/reload, monitor counters, and logging dependencies.
- Low/Moderate: behavior is backward-compatible at API level; main risk is operational differences in logs and startup sequence timing.

## Rollback Plan
If issues arise:
1. Revert `root/core/pom.xml` and `root/web/pom.xml` dependency changes.
2. Revert `root/core/src/main/resources/log4j.properties` to previous logging config.
3. Revert startup/count changes in:
   - `SyncDriver.java`
   - `DeviceLocator.java`
   - `Monitor.java`
   - `Device.java`
   - `DeviceStatsCollector.java`
   - `DeviceLogCleaner.java`

## Working Tree Hygiene
Generated/untracked artifacts observed during earlier development have been cleaned from the workspace.

Current untracked file expected for this change set:
- `PR_DESCRIPTION.md`

Recommendation:
- Keep generated runtime payloads and IDE-specific files out of this PR unless intentionally versioning them.
