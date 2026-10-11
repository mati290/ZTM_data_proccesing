# Kontrakt danych Gold

## Cel i reguły wspólne

Gold odpowiada na pytania biznesowe. Nie kopiuje wierszy Silver; agreguje je
do metryk o jawnie określonym ziarnie. Wszystkie przedziały godzinowe są
godzinami UTC w formacie `[date + hour, date + hour + 1h)`.

Źródłem obserwacji jest
`s3://silver/vehicle_positions/date=YYYY-MM-DD/hour=HH/data.parquet`.
Zawiera ono poprawne, deduplikowane obserwacje. Wiersz jest unikalny w obrębie
godziny dla `(vehicle_type, vehicle_number, gps_time_utc)`. Typ pojazdu
identyfikuje `bus` albo `tram`. Liczba pojazdów zawsze liczy pary
`(vehicle_type, vehicle_number)`, aby jednakowe numery autobusów i tramwajów
nie zostały zlane.

`_completeness.json` z tej samej partycji Silver jest autorytatywnym źródłem
`data_completeness_status`:

- `complete` oznacza, że co najmniej 90% zapisanych manifestów ingestii dla
  godziny ma status `complete` (API zwróciło niepuste dane dla obu typów);
- `incomplete` oznacza, że istnieje co najmniej jeden manifest, ale udział
  kompletnych cykli jest niższy niż 90%;
- `unknown` oznacza brak manifestów albo brak markera.

`complete` nie gwarantuje, że każde wywołanie API zwróciło dane; to status
według progu manifestów, a nie dowód pełnej obserwowalności. `unknown` nigdy
nie jest traktowane jako `complete`.

Wiersze z niekompletnych lub nieznanych godzin można zachować w Gold razem ze
statusem, ale dashboardy nie mogą przedstawiać ich jako wiarygodnych wartości
bez oznaczenia. Widoki prezentujące wyłącznie wiarygodne metryki filtrują
`data_completeness_status = 'complete'`. Brak partycji Gold nie oznacza zera.

## `hourly_network_metrics`

**Cel:** główny szereg czasowy aktywności całej sieci i porównanie jej z
aktywnością autobusów oraz tramwajów.

| Atrybut | Kontrakt |
|---|---|
| Ziarno | Jedna godzina UTC dla całej sieci |
| Klucz unikalności | `(date, hour)` |
| Źródło | Wszystkie wiersze Silver z partycji godziny oraz jej marker kompletności |
| Warunek publikacji | Partycja Silver istnieje i zawiera co najmniej jeden poprawny wiersz; marker można odczytać albo jawnie oznaczyć jako `unknown` |
| Brak danych | Nie tworzyć wiersza z zerami. Brak Silver oznacza brak wyniku; `hourly_data_quality`/orkiestracja ma pokazać lukę, jeśli godzina była oczekiwana |
| Niekompletność | Zachować wyliczone wartości i status. Wykluczyć je z widoków wiarygodnej aktywności albo jawnie oznaczyć |

`vehicle_type` nie jest kolumną tej tabeli, ponieważ jej ziarno to jedna
godzina całej sieci. Podział na typ i linię należy do `hourly_line_metrics`.

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `date` | Data UTC partycji Silver, typ `DATE` |
| `hour` | Godzina UTC partycji, typ `TINYINT`, zakres `0..23` |
| `active_vehicles` | `COUNT(DISTINCT (vehicle_type, vehicle_number))` w wierszach Silver tej godziny |
| `active_bus_vehicles` | Ten sam distinct count z filtrem `vehicle_type = 'bus'` |
| `active_tram_vehicles` | Ten sam distinct count z filtrem `vehicle_type = 'tram'` |
| `position_events` | `COUNT(*)` w Silver; liczba poprawnych zdeduplikowanych obserwacji, nie liczba surowych rekordów Bronze |
| `active_lines` | `COUNT(DISTINCT (vehicle_type, TRIM(line)))` po wykluczeniu `line IS NULL` i pustych wartości |
| `active_brigades` | `COUNT(DISTINCT (vehicle_type, line, brigade))` dla niepustych `line` i `brigade` (brygada ma sens tylko w obrębie linii) |
| `silver_records` | `COUNT(*)` Silver tej godziny (jawny licznik źródła; równy `position_events`) |
| `first_gps_time_utc` | `MIN(gps_time_utc)` w Silver |
| `last_gps_time_utc` | `MAX(gps_time_utc)` w Silver |
| `data_completeness_status` | Status z markera; `unknown`, gdy markeru brak |
| `pipeline_run_at` | Czas UTC wygenerowania wiersza Gold, typ `TIMESTAMPTZ`; nie jest czasem pomiaru GPS |

## `hourly_line_metrics`

**Cel:** porównywanie aktywności linii autobusowych i tramwajowych.

| Atrybut | Kontrakt |
|---|---|
| Ziarno | Jedna godzina UTC × typ pojazdu × linia |
| Klucz unikalności | `(date, hour, vehicle_type, line)` |
| Źródło | Silver z partycji godziny; marker kompletności z tej partycji |
| Warunek publikacji | Co najmniej jeden wiersz Silver dla tego klucza (pusta linia = `unknown`) |
| Brak danych | Nie emitować sztucznego wiersza linii z zerami. Brak linii może oznaczać brak obserwacji lub nieznaną linię |
| Niekompletność | Zachować status godziny dla każdego wiersza; nie porównywać niekompletnego/nieznanego wyniku bez oznaczenia |

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `date`, `hour` | Data i godzina UTC partycji Silver |
| `vehicle_type` | Wartość `bus` albo `tram` ze Silver |
| `line` | `TRIM(line)`; wiersze z `NULL`/pustą linią są liczone razem jako wartość `unknown`, więc `SUM(position_events)` równa się `position_events` sieci |
| `active_vehicles` | `COUNT(DISTINCT vehicle_number)` w obrębie klucza; `vehicle_type` jest częścią klucza |
| `position_events` | `COUNT(*)` w obrębie klucza |
| `active_brigades` | `COUNT(DISTINCT brigade)` niepustych brygad w obrębie klucza (autobusy często nie mają brygady) |
| `first_gps_time_utc` | `MIN(gps_time_utc)` w obrębie klucza |
| `last_gps_time_utc` | `MAX(gps_time_utc)` w obrębie klucza |
| `data_completeness_status` | Status markera całej godziny, wspólny dla wszystkich wierszy godziny |
| `pipeline_run_at` | Czas UTC wygenerowania wiersza Gold |

Jeżeli pojazd zmieni linię w trakcie godziny, jego obserwacje liczą się do każdej
zaobserwowanej linii. Z tego powodu suma `active_vehicles` po liniach może być
większa niż `active_vehicles` w `hourly_network_metrics`.

## `vehicle_activity_hourly`

**Cel:** analiza obserwowanej aktywności konkretnego pojazdu w godzinie.

| Atrybut | Kontrakt |
|---|---|
| Ziarno | Jedna godzina UTC × typ pojazdu × numer pojazdu |
| Klucz unikalności | `(date, hour, vehicle_type, vehicle_number)` |
| Źródło | Silver z partycji godziny; marker kompletności z tej partycji |
| Warunek publikacji | Co najmniej jeden poprawny wiersz Silver dla pojazdu i godziny |
| Brak danych | Nie tworzyć wiersza pojazdu z zerową aktywnością; brak wiersza nie dowodzi, że pojazd nie kursował |
| Niekompletność | Zachować status; brak pojazdu w niekompletnej godzinie nie jest dowodem braku kursowania |

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `date`, `hour` | Data i godzina UTC partycji Silver |
| `vehicle_type`, `vehicle_number` | Identyfikator pojazdu ze Silver; wspólnie rozróżniają pojazdy |
| `position_events` | `COUNT(*)` poprawnych obserwacji pojazdu w tej godzinie |
| `first_gps_time_utc` | `MIN(gps_time_utc)` dla pojazdu |
| `last_gps_time_utc` | `MAX(gps_time_utc)` dla pojazdu |
| `observed_lines` | Posortowany zbiór distinct niepustych `TRIM(line)` obserwowanych w tej godzinie; nie jest jedną przypisaną linią |
| `data_completeness_status` | Status markera godziny |
| `pipeline_run_at` | Czas UTC wygenerowania wiersza Gold |

Kolumny aktywności i położenia (pełna lista w `GOLD_SCHEMAS`):

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `unique_lines` | `COUNT(DISTINCT line)` po wykluczeniu `NULL` |
| `active_seconds` | `date_diff('second', MIN(gps_time_utc), MAX(gps_time_utc))` – obserwowany rozstęp czasu GPS w godzinie, nie czas pracy pojazdu |
| `first_lat`, `first_lon` | `arg_min(lat/lon, gps_time_utc)` – pozycja najwcześniejszej obserwacji (jednoznaczna, bo `gps_time_utc` jest unikalny dla pojazdu) |
| `last_lat`, `last_lon` | `arg_max(lat/lon, gps_time_utc)` |
| `min_lat`, `max_lat`, `min_lon`, `max_lon` | `MIN`/`MAX` współrzędnych w godzinie |

Przebyte kilometry nie są liczone: wymagają sortowania, filtrowania skoków GPS
i osobnej definicji jakości.

## `hourly_data_quality`

**Cel:** odróżnienie zmian aktywności sieci od problemów z pobieraniem lub
przetwarzaniem danych.

| Atrybut | Kontrakt |
|---|---|
| Ziarno | Jedna oczekiwana godzina UTC |
| Klucz unikalności | `(date, hour)` |
| Źródło | Silver, kwarantanna Silver, marker Silver oraz manifesty Bronze |
| Warunek publikacji | Publikowany zawsze jako ostatni (rekord zatwierdzający godzinę), także gdy Silver jest brakujący lub pusty - wtedy zadanie następnie kończy się błędem |
| Brak danych | Wygenerować rekord jakości z jawnym `silver_partition_status`; nie zastępować nieznanych liczników zerami |
| Niekompletność | Zachować wszystkie dostępne liczniki, status kompletności i status runu. Nie nazywać takich metryk wiarygodną aktywnością sieci |

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `date`, `hour` | Partycja UTC godziny (`data_interval_start` runu Airflow) |
| `silver_partition_status` | `present` gdy `data.parquet` istnieje, `missing` w przeciwnym razie |
| `data_completeness_status` | Status `_completeness.json`; `unknown` gdy pliku nie ma lub nie da się potwierdzić statusu |
| `manifest_cycles` | Liczba manifestów Bronze znalezionych dla godziny |
| `complete_manifest_cycles` | Liczba tych manifestów z `status = 'complete'` |
| `complete_cycle_ratio` | `complete_manifest_cycles / manifest_cycles`; `NULL`, gdy `manifest_cycles = 0` |
| `partial_manifest_cycles` | `manifest_cycles - complete_manifest_cycles`; `NULL`, gdy liczba cykli jest nieznana |
| `bronze_records` | `silver_position_events + quarantined_records` (zastępuje `records_processed`). Jest to dokładna liczba rekordów Bronze po odczycie, bo transformacja Bronze→Silver weryfikuje `przed - po = odrzucone`; `NULL`, gdy którykolwiek składnik jest nieznany |
| `silver_position_events` | `COUNT(*)` z Silver; `NULL`, gdy partycja Silver nie istnieje |
| `quarantined_records` | `COUNT(*)` z partycji kwarantanny; `NULL`, gdy plik kwarantanny nie istnieje lub jest niedostępny |
| `rejection_pct` | `100 * quarantined_records / bronze_records` (skala 0–100); `NULL`, gdy `bronze_records` jest `NULL` lub 0. Zawiera odrzucenia rutynowe (ok. 40% to `stale_gps_time`), więc nie jest progiem jakości |
| `duplicate_records` | Liczba wierszy kwarantanny z `rejection_reason = 'duplicate'`; `NULL`, gdy brak kwarantanny |
| `integrity_rejection_pct` | Jak `rejection_pct`, ale bez przyczyn rutynowych (`duplicate`, `stale_gps_time`), tj. `invalid_coordinates`, `future_gps_time` itp. |
| `quarantined_by_reason` | Liczności `GROUP BY rejection_reason` z kwarantanny; pusta lista tylko wtedy, gdy kwarantanna została odczytana i nie zawiera wierszy |
| `safe_for_dashboard` | `TRUE` wtedy i tylko wtedy, gdy `data_completeness_status = 'complete'`, Silver ma co najmniej jeden wiersz, `is_final = TRUE`, `bronze_records` jest znane i `integrity_rejection_pct <= 5`. Nigdy `NULL` (brak informacji = `FALSE`) |
| `pipeline_run_at` | Czas UTC utworzenia wiersza jakości |

Sama lista partycji Bronze, Silver i manifestów nie pozwala wykryć godziny,
w której ingestion całkowicie nie działał: dla takiej godziny nie ma obiektu do
odkrycia. Dlatego `hourly_data_quality` musi korzystać z oczekiwanych godzin i
statusów Airflow. Jeżeli dostęp do metadanych Airflow nie zostanie włączony do
Gold, brak wiersza jakości oznacza „brak informacji”, a nie zero pojazdów ani
brak aktywności.

## Reguły interpretacji dashboardu

1. Dashboard domyślnie pokazuje wyłącznie godziny z
   `hourly_data_quality.safe_for_dashboard = TRUE`; pozostałe godziny
   oznacza wizualnie (np. kolor/wzór), nie ukrywa ich bez śladu.
2. Brak `hourly_network_metrics` nie jest zerem. Dashboard łączy oś oczekiwanych
   godzin z `hourly_data_quality` i pokazuje brakującą partycję jako lukę.
3. Wykres liczby pojazdów opisuje liczbę unikalnych identyfikatorów
   zaobserwowanych w godzinie, nie liczbę wszystkich pojazdów w ruchu ani czas
   ich aktywności.
4. `position_events`, `active_lines` i liczniki jakości można odtworzyć z
   opisanych źródeł i kluczy przy użyciu `COUNT`, `COUNT(DISTINCT)`, `MIN`,
   `MAX` oraz `GROUP BY`; nie są ręcznie interpretowanymi ocenami.
5. Wszystkie tabele Gold są idempotentnie odtwarzane dla klucza godzinowego.
   `pipeline_run_at` jest metadanymi wykonania i nie stanowi części klucza.
6. Każdy wiersz Gold ma `is_final` i `source_max_ingested_at`. Godzina jest
   finalna (`is_final = TRUE`), gdy czas runu ≥ koniec godziny + 15 min
   (`FINALITY_GRACE`); wcześniej jest nadal otwarta na późne dane.
   `safe_for_dashboard` wymaga dodatkowo `is_final = TRUE`.
   `source_max_ingested_at` to największy `ingested_at` w Silver użytym do
   budowy partycji (`NULL`, gdy Silver nie istnieje); wzrost tej wartości po
   ponownym przeliczeniu oznacza, że dotarły późne dane.
7. Polityka korekt: DAG `reprocess_late_data_hourly` (co godzinę, `:30`)
   przelicza Silver i Gold dla zamkniętych godzin z ostatnich 24 h, w których
   obiekty Bronze są nowsze niż partycja Silver (lub Silver brakuje). Starsze
   godziny są zamrożone. Nadpisanie partycji jest atomowe (server-side copy)
   i idempotentne; równoległy przebieg z głównymi DAG-ami jest bezpieczny.


## Implementacja (silver_to_gold.py)

- Kolejność publikacji: `vehicle_activity_hourly`, `hourly_line_metrics`,
  `hourly_network_metrics`, na końcu `hourly_data_quality`. Pliki są
  najpierw zapisywane w `s3://gold/_staging/`, walidowane (schemat, liczba
  wierszy), kopiowane do `<table>/date=D/hour=HH/data.parquet` i usuwane ze
  stagingu. Obecność wiersza jakości oznacza zakończoną publikację godziny.
- Idempotencja: ponowny run godziny nadpisuje te same klucze i daje te same
  kolumny danych; `pipeline_run_at` jest metadaną runu i się zmienia.
- Walidacje: schemat Silver przed agregacją, spójność krzyżowa tabel
  (suma pojazdów, zdarzeń) oraz unikalność kluczy.
- Godziny `incomplete`/`unknown` są publikowane z jawnym
  `data_completeness_status`; konsument filtruje `complete`.
- Zakres jakości nie obejmuje już statusu Airflow; luki w godzinach wykrywa
  się orkiestracją (nieudany run) i osią oczekiwanych godzin na dashboardzie.
