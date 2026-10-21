# Gold data contract

**[English](gold-data-contract.en.md) · [Polski](gold-data-contract.md)** · [Project](../README.md)

Implementation documentation as of 21 October 2026, based on `529cc77`. This describes current behavior, not an implemented dashboard or an SLA. Physical schemas and keys are defined in [silver_to_gold.py](../silver_to_gold.py) (`GOLD_SCHEMAS`, `GOLD_KEYS`); input rules in [bronze_to_silver.py](../bronze_to_silver.py) and [ingestion_contract.py](../ingestion_contract.py).

## 1. Time boundaries and identity

The processing unit is the UTC collection-slot hour: `[date + hour, date + hour + 1h)`. It is not a GPS event-time hour. Both sources and their cycle manifest retain that slot partition across hour/day boundaries. `_ingested_at` is the actual write time. Historical objects retain their previous partition layout.

A vehicle is identified by `(vehicle_type, vehicle_number)`, with bus/tram types. Silver deduplicates `(vehicle_type, vehicle_number, gps_time_utc)` within one partition hour only. The latest ingested_at wins, with additional ordering for ties. A GPS observation can recur in adjacent collection hours.

## 2. Inputs, manifests and Silver validation

- Bronze: JSONL preserving API fields and adding `_ingested_at`, under `vehicle_type=bus|tram/date=.../hour=.../`.
- The producer emits v2 cycle manifests: source counts/status, failed_sources, empty_sources, cycle/slot/write timestamps, poll_interval_seconds and bus/tram source_objects. Each nonempty source identifies its object key, positive byte size and ETag when returned by storage. Complete requires both sources to be nonempty; partial describes other cycles that reached the manifest-write step.
- Consumers check types, required fields, status/count/list consistency, slot alignment and referenced objects against selected inputs. Missing objects or changed sizes/known ETags stop the build. V1/unversioned manifests retain legacy compatibility, without inventing metadata that was never recorded.
- JSON columns are explicit. Selected data is materialized once for quality, Silver and quarantine; completeness uses the same selected manifest set.
- Coordinates: lat 51.9–52.6, lon 20.5–21.6; nonblank vehicle_number. GPS source time is local Europe/Warsaw converted to UTC. Invalid, ambiguous and nonexistent DST times are rejected; valid GPS age relative to ingestion is 0–5 minutes inclusive.
- Each rejected row gets exactly one reason: `invalid_coordinates`, `missing_vehicle_number`, `ambiguous_local_time`, `nonexistent_local_time`, `missing_or_invalid_timestamp`, `future_gps_time`, `stale_gps_time`, `duplicate`.
- Gold validates Silver schema/content, observation uniqueness and the type/presence/allowed values of rejection_reason. Read and validation failures are not converted into unknown or zero.

If every loaded record is rejected, Silver writes versioned diagnostic quarantine under `_diagnostics`, does not activate empty Silver, and raises an error. A readable previous publication can remain active; its presence does not establish that the latest attempt succeeded.

## 3. Reading committed publications

Silver: `s3://silver/vehicle_positions/date=YYYY-MM-DD/hour=HH/_active.json` points to artifacts.data, artifacts.quarantine and artifacts.completeness under `_versions/<partition>/<publication_id>/`. The active manifest also records source_inputs: keys, sizes, ETags and modification times of selected Bronze data and cycle manifests. Without the pointer, legacy fixed paths are supported.

Gold: `s3://gold/_commits/date=YYYY-MM-DD/hour=HH/manifest.json` contains publication_id, partition, source_silver_uri, is_final and all four tables with uri/rows. For legacy inputs it also records source_silver_etag and source_silver_identity for data, quarantine and completeness, including absent files.

Consumer procedure:

1. Establish expected hours independently of existing objects.
2. Read one commit per hour and use its exact URIs for all tables.
3. Join quality and metrics within the same hour **and publication**. Do not glob all `_versions` or re-resolve the pointer between table reads.
4. Key caches by publication_id/commit identity. A missing commit/file is a gap/error, not permission to silently fall back to another version.
5. Show freshness and processing status separately from stored quality flags.

Publication stages and validates files, copies them to new paths, then replaces one manifest. This is the commit boundary, not a transaction across every storage object. Failure before commit preserves the previous manifest. Old/orphan versions need retention that respects reader references.

For absent/empty Silver, Gold can publish only the quality table and a separate `_diagnostics/<partition>/manifest.json` with silver_missing/silver_empty status, then fail the task. Diagnostics do not replace a full commit and do not guarantee a quality row for every expected hour.

## 4. Completeness and finality

The default 30 s interval gives 120 slots per closed hour. Expected counts follow epoch-aligned UTC slots and assessment time; an open hour includes only slots that have started. V2 manifests record the interval. History without a recorded policy uses 30 s. Mixed policies within an hour require explicit migration; today's environment must not reinterpret historical inputs.

| Status | Rule |
|---|---|
| complete | ≥95% expected-slot coverage and ≥90% complete slots among observed valid slots |
| incomplete | Manifests exist but thresholds are not met |
| unknown | No manifests or no completeness marker |

A slot is complete when at least one manifest for it is complete. Duplicates do not inflate unique coverage. Future, out-of-hour, invalid or misaligned slots do not fill gaps. If slot metadata exists, manifests without it do not fill missing slots. Only hours with no slot field anywhere use legacy manifest-count assessment; unique-slot metrics then remain NULL.

Gold rejects contradictory complete markers and inconsistent counters, including insufficient legacy complete evidence. Counters are nonnegative integers, not bools; complete_slots ≤ observed_slots ≤ expected_cycles, observed_slots ≤ cycles and complete_slots ≤ complete_cycles. Gold calculates ratios from counters, not an arbitrary recorded percentage.

`is_final=true` only means build time ≥ hour end + 15 min. Corrections may still change results. The repair DAG runs at minute 30 and finalizes eligible hours without new Bronze; publication exactly at minute 15 is not promised.

## 5. Table schemas

All four tables contain the common fields below. GOLD_SCHEMAS validates physical order and the full column set; common columns are separated here for readability. Keys identify one publication; storage may contain multiple historical versions of a logical key.

| Column | Type | Meaning |
|---|---|---|
| `date` | `DATE` | UTC date of the collection-slot partition. |
| `hour` | `TINYINT` | UTC hour 0–23; not a local or GPS hour. |
| `data_completeness_status` | `VARCHAR` | complete / incomplete / unknown, after Silver marker validation. |
| `is_final` | `BOOLEAN` | Build time ≥ hour end + 15 min; does not close the correction window. |
| `source_max_ingested_at` | `TIMESTAMP WITH TIME ZONE` | MAX(ingested_at) of selected Silver; NULL for missing/empty Silver diagnostics. Not a version identifier. |
| `pipeline_run_at` | `TIMESTAMP WITH TIME ZONE` | Shared UTC build time for the hourly publication; not part of its business key. |

### `hourly_network_metrics`

**Key:** `(date, hour)`.

| Column | Type | Meaning |
|---|---|---|
| `active_vehicles` | `BIGINT` | Distinct vehicles: (type, number) for the network; number within a line. |
| `active_bus_vehicles` | `BIGINT` | Distinct vehicle_number for bus. |
| `active_tram_vehicles` | `BIGINT` | Distinct vehicle_number for tram. |
| `position_events` | `BIGINT` | COUNT(*) of valid, deduplicated Silver rows in the group. |
| `active_lines` | `BIGINT` | Distinct (vehicle_type, line), excluding missing lines. |
| `active_brigades` | `BIGINT` | Distinct (type, line, brigade) for the network with known line/brigade; distinct known brigades within a line group. |
| `silver_records` | `BIGINT` | COUNT(*) of hourly Silver; equals network position_events. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) in the group. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) in the group. |

### `hourly_line_metrics`

**Key:** `(date, hour, vehicle_type, line)`.

| Column | Type | Meaning |
|---|---|---|
| `vehicle_type` | `VARCHAR` | bus or tram; part of vehicle/line identity. |
| `line` | `VARCHAR` | Trimmed line; NULL/blank values grouped as unknown. |
| `active_vehicles` | `BIGINT` | Distinct vehicles: (type, number) for the network; number within a line. |
| `position_events` | `BIGINT` | COUNT(*) of valid, deduplicated Silver rows in the group. |
| `active_brigades` | `BIGINT` | Distinct (type, line, brigade) for the network with known line/brigade; distinct known brigades within a line group. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) in the group. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) in the group. |

### `vehicle_activity_hourly`

**Key:** `(date, hour, vehicle_type, vehicle_number)`.

| Column | Type | Meaning |
|---|---|---|
| `vehicle_type` | `VARCHAR` | bus or tram; part of vehicle/line identity. |
| `vehicle_number` | `VARCHAR` | Nonblank vehicle identifier; combine with vehicle_type. |
| `position_events` | `BIGINT` | COUNT(*) of valid, deduplicated Silver rows in the group. |
| `unique_lines` | `BIGINT` | COUNT(DISTINCT line), excluding NULL. |
| `observed_lines` | `VARCHAR[]` | Sorted set of known lines; [] when no line is known. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) in the group. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) in the group. |
| `active_seconds` | `BIGINT` | date_diff in seconds between first and last GPS; observation span, not driving time. |
| `first_lat` | `DOUBLE` | arg_min(lat, gps_time_utc). |
| `first_lon` | `DOUBLE` | arg_min(lon, gps_time_utc). |
| `last_lat` | `DOUBLE` | arg_max(lat, gps_time_utc). |
| `last_lon` | `DOUBLE` | arg_max(lon, gps_time_utc). |
| `min_lat` | `DOUBLE` | MIN(lat) within the vehicle group. |
| `max_lat` | `DOUBLE` | MAX(lat) within the vehicle group. |
| `min_lon` | `DOUBLE` | MIN(lon) within the vehicle group. |
| `max_lon` | `DOUBLE` | MAX(lon) within the vehicle group. |

### `hourly_data_quality`

**Key:** `(date, hour)`.

| Column | Type | Meaning |
|---|---|---|
| `silver_partition_status` | `VARCHAR` | present when the Silver file exists (including empty); missing otherwise. |
| `manifest_cycles` | `BIGINT` | All cycle manifests recorded by the marker, including repeated slots; NULL when unknown. |
| `complete_manifest_cycles` | `BIGINT` | Manifests with complete status; NULL when unknown. |
| `partial_manifest_cycles` | `BIGINT` | manifest_cycles − complete_manifest_cycles; NULL when counters are unknown. |
| `complete_cycle_ratio` | `DOUBLE` | complete_manifest_cycles / manifest_cycles; NULL for an unknown/zero denominator. |
| `expected_slots` | `BIGINT` | Marker expected_cycles; 120 for a closed hour at 30 s; NULL when unknown. |
| `observed_slots` | `BIGINT` | Unique valid UTC slots; NULL for legacy without slot metadata. |
| `complete_slots` | `BIGINT` | Unique slots with at least one complete manifest; NULL when unknown. |
| `slot_coverage_ratio` | `DOUBLE` | observed_slots / expected_slots; scale 0–1, NULL for unknown/zero denominator. |
| `complete_slot_ratio` | `DOUBLE` | complete_slots / observed_slots; scale 0–1, NULL for unknown/zero denominator. |
| `bronze_records` | `BIGINT` | silver_position_events + quarantined_records; NULL if an input count is unknown. |
| `silver_position_events` | `BIGINT` | Rows in loaded Silver; 0 if empty, NULL if missing. |
| `quarantined_records` | `BIGINT` | COUNT(*) of quarantine; NULL when the file is absent. |
| `rejection_pct` | `DOUBLE` | 100 × quarantined_records / bronze_records; scale 0–100; NULL for unknown/zero base. |
| `duplicate_records` | `BIGINT` | Quarantine rows with duplicate reason; NULL when quarantine is unknown. |
| `integrity_rejection_pct` | `DOUBLE` | Rejection percentage excluding duplicate and stale_gps_time, relative to bronze_records. |
| `quarantined_by_reason` | `STRUCT(reason VARCHAR, records BIGINT)[]` | Sorted {reason, records} structs; [] for loaded empty quarantine, NULL for absent file. |
| `safe_for_dashboard` | `BOOLEAN` | Result of the completeness, finality and integrity policy below; never NULL. |

## 6. The safe_for_dashboard rule

The current implementation requires all of:

- data_completeness_status=complete;
- positive known expected slots/cycles, sufficient 95% coverage and 90% completeness evidence (corresponding cycle evidence for legacy);
- nonempty Silver and is_final=true;
- known integrity_rejection_pct ≤5%; missing evidence yields FALSE, never NULL.

`duplicate` and `stale_gps_time` do not contribute to integrity_rejection_pct. Even a very high stale-GPS share can therefore coexist with TRUE. This flag is not a guarantee of fleet-wide freshness, complete upstream coverage or success of the latest run. A freshness indicator and any additional thresholds remain prerequisites for the dashboard. The is_hour_complete helper does not replace this policy.

## 7. Invariants and analytical interpretation

- Network active_vehicles = active_bus_vehicles + active_tram_vehicles = vehicle_activity_hourly row count.
- Network position_events = sum of vehicle position_events = sum of line position_events = silver_records.
- A vehicle changing lines can make the sum of per-line vehicle counts exceed the network count.
- Do not sum hourly distinct counts into daily unique vehicles. Observation span is not working time.
- A local day can contain 23/25 hours. Keys/joins remain UTC; repeated local-hour labels need offsets.
- No observation of a line/vehicle does not establish that it was not operating. A missing hour/quality row is a gap/unknown, not zero.
- Make provisional/incomplete/unknown hours visible; a safe-only filter must not silently remove them from the time axis.
- Distance, timetable punctuality, schedule delays and a complete live map are not computed; these need additional sources/rules.

## 8. Corrections, immutability and compatibility

Repair scans the previous 24 closed hours by default. Changed input lists or missing required Silver files schedule a rebuild. Changed/deleted previously used Bronze objects block rebuilding until restored. Gold is repaired without rebuilding current Silver when the commit is absent/outdated, table set/URIs are wrong, a referenced file is absent or finalization is due. Mutable legacy inputs have identity checks before/after reading.

Rebuilds reproduce logical results, not identical bytes: UUIDs and pipeline_run_at can change. Source objects and published versions are assumed immutable. A metadata snapshot is not a transactional versioned read of every object; the pool serializes cooperating Airflow writers only. Manual writes need the same coordination.

Catchup is disabled. History outside the correction window needs controlled backfill. Retention, a durable collection-start registry, a full expected-hour axis and Gold manifest schema versioning remain development tasks. Contract changes require producer/consumer migration planning; schema_version=2 refers to ingestion manifests, not every Gold table.

## 9. Contract verification

Tests: [test_bronze_to_silver.py](../tests/test_bronze_to_silver.py), [test_silver_to_gold.py](../tests/test_silver_to_gold.py), [test_late_data.py](../tests/test_late_data.py), [test_review_regressions.py](../tests/test_review_regressions.py). Storage/failure scenarios: [pipeline_e2e.py](../tests/pipeline_e2e.py). No-API-key instructions: [README](../README.md#demo-without-a-ztm-api-key).

Tests cover schemas/keys, reconciled counts, completeness, rejections, commit boundaries, repair and finality. They do not establish complete API coverage, continuous availability, backup readiness or performance at arbitrary scale.
