# Kontrakt danych Gold

**[Polski](gold-data-contract.md) · [English](gold-data-contract.en.md)** · [Projekt](../README.pl.md)

Dokumentacja implementacji z 21 października 2026, bazująca na `529cc77`. Jest opisem bieżącego zachowania, nie deklaracją zaimplementowanego dashboardu ani SLA. Schematy fizyczne i klucze definiuje [silver_to_gold.py](../silver_to_gold.py) (`GOLD_SCHEMAS`, `GOLD_KEYS`); reguły wejścia — [bronze_to_silver.py](../bronze_to_silver.py) i [ingestion_contract.py](../ingestion_contract.py).

## 1. Granice czasu i tożsamość

Jednostką przetwarzania jest godzina UTC slotu pobierania: `[date + hour, date + hour + 1h)`. Nie jest to godzina zdarzenia GPS. Autobusy, tramwaje i manifest jednego cyklu zachowują partycję slotu także przy przekroczeniu godziny lub północy. `_ingested_at` jest rzeczywistym czasem zapisu. Historyczne dane zachowują wcześniejszy układ partycji.

Numery pojazdów są rozróżniane przez `(vehicle_type, vehicle_number)`, gdzie typ to bus/tram. Deduplikacja Silver odbywa się tylko w obrębie partycji godziny dla `(vehicle_type, vehicle_number, gps_time_utc)`. Wygrywa najnowsze ingested_at, z dodatkowymi polami sortowania przy remisie. Ta sama obserwacja GPS może wystąpić w sąsiednich godzinach pobrania.

## 2. Wejście, manifesty i walidacja Silver

- Bronze: JSONL z polami API i `_ingested_at`, pod `vehicle_type=bus|tram/date=.../hour=.../`.
- Producent zapisuje manifesty cykli v2: status/liczniki źródeł, failed_sources, empty_sources, czasy cyklu/slotu/zapisu, poll_interval_seconds i source_objects dla bus/tram. Niepuste źródło zawiera klucz obiektu, dodatni rozmiar i ETag, jeśli zwrócił go storage. Status complete wymaga obu niepustych źródeł; partial opisuje pozostałe cykle, które dotarły do zapisu manifestu.
- Konsument sprawdza typy, wymagane pola, spójność statusu/liczników/list, slot i powiązanie wskazanych obiektów z wybranym wejściem. Brak danych źródłowych albo zmiana rozmiaru/znanego ETag przerywa budowę. V1 i niewersjonowane manifesty mają kompatybilność historyczną; nie zyskują informacji, której dawniej nie zapisano.
- Schemat odczytu JSON jest jawny. Wybrane pliki są materializowane raz dla jakości, Silver i kwarantanny. Marker kompletności korzysta z tego samego wybranego zestawu manifestów.
- Współrzędne: lat 51.9–52.6, lon 20.5–21.6; niepusty vehicle_number. GPS źródła jest lokalnym czasem Europe/Warsaw, przeliczanym na UTC. Czasy błędne, niejednoznaczne i nieistniejące przy DST są odrzucane; dopuszczalny wiek względem ingestii wynosi 0–5 min włącznie.
- Kwarantanna zawiera dokładnie jeden powód na odrzucony wiersz: `invalid_coordinates`, `missing_vehicle_number`, `ambiguous_local_time`, `nonexistent_local_time`, `missing_or_invalid_timestamp`, `future_gps_time`, `stale_gps_time`, `duplicate`.
- Gold weryfikuje schema i treść Silver, unikalność obserwacji oraz typ/obecność/dozwolone wartości rejection_reason. Błędy nie są zamieniane na unknown ani zero.

Gdy wszystkie odczytane rekordy zostaną odrzucone, Silver zapisuje wersjonowaną diagnostyczną kwarantannę w `_diagnostics`, ale nie aktywuje pustego Silver i zgłasza błąd. Czytelna wcześniejsza publikacja może pozostać aktywna — jej obecność nie dowodzi sukcesu ostatniej próby.

## 3. Odczyt zatwierdzonych publikacji

Silver: `s3://silver/vehicle_positions/date=YYYY-MM-DD/hour=HH/_active.json` wskazuje artifacts.data, artifacts.quarantine i artifacts.completeness pod `_versions/<partycja>/<publication_id>/`. Aktywny manifest przechowuje również source_inputs: klucze, rozmiary, ETag i czasy modyfikacji wybranych danych oraz manifestów Bronze. Bez wskaźnika obsługiwane są historyczne stałe ścieżki.

Gold: `s3://gold/_commits/date=YYYY-MM-DD/hour=HH/manifest.json` zawiera publication_id, partition, source_silver_uri, is_final i wszystkie cztery tables z uri/rows. Dla legacy zapisywana jest też source_silver_etag i source_silver_identity dla danych, kwarantanny i markera (również nieobecności pliku).

Procedura konsumenta:

1. Ustal oczekiwane godziny niezależnie od tego, które obiekty istnieją.
2. Odczytaj jeden manifest commitu na godzinę i użyj dokładnych URI wszystkich tabel z tej publikacji.
3. Połącz jakość i metryki w obrębie tej samej godziny **i publikacji**. Nie globuj całego `_versions` ani nie czytaj ponownie wskaźnika między tabelami.
4. Cache identyfikuj przez publication_id/tożsamość commitu. Brak commitu lub pliku to błąd/luka; nie wracaj po cichu do innej wersji.
5. Pokazuj aktualność i stan przetwarzania osobno od zapisanych flag jakości.

Publikacja przygotowuje i waliduje staging, kopiuje pliki do nowych ścieżek, a następnie zastępuje jeden manifest. To granica zatwierdzenia; nie transakcja obejmująca wszystkie obiekty storage. Awaria przed commitem zachowuje poprzedni commit. Starsze i osierocone wersje wymagają retencji respektującej referencje czytelników.

Przy brakującym albo pustym Silver Gold może zapisać samą tabelę jakości i oddzielny manifest `_diagnostics/<partycja>/manifest.json` ze statusem silver_missing/silver_empty, po czym kończy zadanie błędem. Diagnostyka nie zastępuje pełnego commitu i nie gwarantuje wiersza dla każdej oczekiwanej godziny.

## 4. Kompletność i finalizacja

Domyślnie 30 s oznacza 120 slotów w zakończonej godzinie. Oczekiwana liczba wynika z wyrównania slotów do epoki UTC i czasu oceny; w bieżącej godzinie obejmuje tylko rozpoczęte sloty. Manifesty v2 utrwalają interwał. Bez zapisanej polityki przyjęte jest historyczne 30 s. Mieszane polityki w godzinie wymagają jawnej migracji; nie reinterpretujemy historii nową zmienną środowiskową.

| Status | Reguła |
|---|---|
| complete | ≥95% oczekiwanych slotów i ≥90% kompletnych spośród obecnych poprawnych slotów |
| incomplete | Manifesty są obecne, ale progi nie są spełnione |
| unknown | Brak manifestów albo brak markera kompletności |

Slot jest kompletny, gdy co najmniej jeden jego manifest jest complete. Duplikaty nie zwiększają unikalnego pokrycia. Sloty z przyszłości, spoza godziny, niepoprawne lub niewyrównane nie wypełniają luk. Jeżeli występują metadane slotów, manifesty bez nich nie uzupełniają braków. Wyłącznie godziny całkowicie bez pola slotu korzystają z historycznych liczników manifestów; metryki slotów pozostają wtedy NULL.

Gold odrzuca sprzeczne markery complete i niespójne liczniki; sprawdza także wystarczające dowody dla legacy complete. Liczniki są nieujemnymi int, nie bool; complete_slots ≤ observed_slots ≤ expected_cycles, observed_slots ≤ cycles oraz complete_slots ≤ complete_cycles. Proporcje Gold są liczone z liczników, nie z dowolnie wpisanego procentu.

`is_final=true` oznacza wyłącznie czas budowy ≥ koniec godziny + 15 min. Korekty nadal mogą zmienić wynik. DAG napraw działa w minucie 30 i finalizuje także bez nowych danych Bronze; nie jest to obietnica publikacji dokładnie w minucie 15.

## 5. Schematy tabel

Wszystkie cztery tabele zawierają wspólne pola poniżej. Kolejność fizyczną i pełny zestaw pól sprawdza GOLD_SCHEMAS; poniżej wspólne pola oddzielono dla czytelności. Klucze opisują jedną publikację — w storage może istnieć wiele historycznych wersji tego samego klucza.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `date` | `DATE` | Data UTC partycji slotu pobierania. |
| `hour` | `TINYINT` | Godzina UTC 0–23; nie godzina lokalna ani GPS. |
| `data_completeness_status` | `VARCHAR` | complete / incomplete / unknown, po walidacji markera Silver. |
| `is_final` | `BOOLEAN` | Czas budowy ≥ koniec godziny + 15 min; nie zamyka okna korekt. |
| `source_max_ingested_at` | `TIMESTAMP WITH TIME ZONE` | MAX(ingested_at) użytego Silver; NULL dla diagnostyki braku/pustego Silver. Nie identyfikuje wersji. |
| `pipeline_run_at` | `TIMESTAMP WITH TIME ZONE` | Wspólny czas UTC budowy publikacji godziny, poza kluczem biznesowym. |

### `hourly_network_metrics`

**Klucz:** `(date, hour)`.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `active_vehicles` | `BIGINT` | Liczba distinct pojazdów: para (typ, numer) w sieci; numer w obrębie linii. |
| `active_bus_vehicles` | `BIGINT` | Distinct vehicle_number dla bus. |
| `active_tram_vehicles` | `BIGINT` | Distinct vehicle_number dla tram. |
| `position_events` | `BIGINT` | COUNT(*) poprawnych, zdeduplikowanych wierszy Silver w danej grupie. |
| `active_lines` | `BIGINT` | Distinct (vehicle_type, line), pomijając brak linii. |
| `active_brigades` | `BIGINT` | Distinct (typ, linia, brygada) w sieci przy znanej linii i brygadzie; distinct znanych brygad w grupie linii. |
| `silver_records` | `BIGINT` | COUNT(*) Silver godziny; równe sieciowemu position_events. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) w grupie. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) w grupie. |

### `hourly_line_metrics`

**Klucz:** `(date, hour, vehicle_type, line)`.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `vehicle_type` | `VARCHAR` | bus albo tram; część tożsamości pojazdu/linii. |
| `line` | `VARCHAR` | Przycięta linia; NULL/pusta grupowana pod unknown. |
| `active_vehicles` | `BIGINT` | Liczba distinct pojazdów: para (typ, numer) w sieci; numer w obrębie linii. |
| `position_events` | `BIGINT` | COUNT(*) poprawnych, zdeduplikowanych wierszy Silver w danej grupie. |
| `active_brigades` | `BIGINT` | Distinct (typ, linia, brygada) w sieci przy znanej linii i brygadzie; distinct znanych brygad w grupie linii. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) w grupie. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) w grupie. |

### `vehicle_activity_hourly`

**Klucz:** `(date, hour, vehicle_type, vehicle_number)`.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `vehicle_type` | `VARCHAR` | bus albo tram; część tożsamości pojazdu/linii. |
| `vehicle_number` | `VARCHAR` | Niepusty identyfikator pojazdu; łączyć z vehicle_type. |
| `position_events` | `BIGINT` | COUNT(*) poprawnych, zdeduplikowanych wierszy Silver w danej grupie. |
| `unique_lines` | `BIGINT` | COUNT(DISTINCT line), bez NULL. |
| `observed_lines` | `VARCHAR[]` | Posortowany zbiór znanych linii; [] przy braku znanych linii. |
| `first_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MIN(gps_time_utc) w grupie. |
| `last_gps_time_utc` | `TIMESTAMP WITH TIME ZONE` | MAX(gps_time_utc) w grupie. |
| `active_seconds` | `BIGINT` | date_diff w sekundach między pierwszym i ostatnim GPS; rozstęp obserwacji, nie czas jazdy. |
| `first_lat` | `DOUBLE` | arg_min(lat, gps_time_utc). |
| `first_lon` | `DOUBLE` | arg_min(lon, gps_time_utc). |
| `last_lat` | `DOUBLE` | arg_max(lat, gps_time_utc). |
| `last_lon` | `DOUBLE` | arg_max(lon, gps_time_utc). |
| `min_lat` | `DOUBLE` | MIN(lat) w grupie pojazdu. |
| `max_lat` | `DOUBLE` | MAX(lat) w grupie pojazdu. |
| `min_lon` | `DOUBLE` | MIN(lon) w grupie pojazdu. |
| `max_lon` | `DOUBLE` | MAX(lon) w grupie pojazdu. |

### `hourly_data_quality`

**Klucz:** `(date, hour)`.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `silver_partition_status` | `VARCHAR` | present, gdy plik Silver istnieje (także pusty); missing przy jego braku. |
| `manifest_cycles` | `BIGINT` | Wszystkie manifesty cykli z markera, także powtórzone sloty; NULL przy braku dowodu. |
| `complete_manifest_cycles` | `BIGINT` | Manifesty ze statusem complete; NULL przy braku dowodu. |
| `partial_manifest_cycles` | `BIGINT` | manifest_cycles − complete_manifest_cycles; NULL, gdy liczniki nieznane. |
| `complete_cycle_ratio` | `DOUBLE` | complete_manifest_cycles / manifest_cycles; NULL dla braku/zera mianownika. |
| `expected_slots` | `BIGINT` | expected_cycles markera; 120 dla zakończonej godziny przy interwale 30 s; NULL, gdy brak dowodu. |
| `observed_slots` | `BIGINT` | Unikalne poprawne sloty UTC; NULL w legacy bez metadanych slotów. |
| `complete_slots` | `BIGINT` | Unikalne sloty z co najmniej jednym kompletnym manifestem; NULL, gdy nieznane. |
| `slot_coverage_ratio` | `DOUBLE` | observed_slots / expected_slots; skala 0–1, NULL przy nieznanym/zerowym mianowniku. |
| `complete_slot_ratio` | `DOUBLE` | complete_slots / observed_slots; skala 0–1, NULL przy nieznanym/zerowym mianowniku. |
| `bronze_records` | `BIGINT` | silver_position_events + quarantined_records; NULL przy nieznanym składniku. |
| `silver_position_events` | `BIGINT` | Liczba wierszy odczytanego Silver; 0 dla pustego, NULL dla brakującego. |
| `quarantined_records` | `BIGINT` | COUNT(*) kwarantanny; NULL, gdy pliku nie ma. |
| `rejection_pct` | `DOUBLE` | 100 × quarantined_records / bronze_records; skala 0–100; NULL przy braku/zerze podstawy. |
| `duplicate_records` | `BIGINT` | Wiersze kwarantanny z powodem duplicate; NULL, gdy kwarantanna nieznana. |
| `integrity_rejection_pct` | `DOUBLE` | Procent odrzuceń bez duplicate i stale_gps_time, względem bronze_records. |
| `quarantined_by_reason` | `STRUCT(reason VARCHAR, records BIGINT)[]` | Posortowane struktury {reason, records}; [] dla odczytanej pustej kwarantanny, NULL gdy pliku brak. |
| `safe_for_dashboard` | `BOOLEAN` | Wynik polityki kompletności, finalizacji i integralności opisanej poniżej; nigdy NULL. |

## 6. Reguła safe_for_dashboard

Aktualna implementacja wymaga łącznie:

- data_completeness_status=complete;
- dodatniej znanej oczekiwanej liczby slotów/cykli, dostatecznego pokrycia 95% i udziału kompletnych 90% (dla legacy odpowiednie dowody cykli);
- niepustego Silver i is_final=true;
- znanego integrity_rejection_pct ≤5%; brak dowodu daje FALSE, nigdy NULL.

`duplicate` i `stale_gps_time` nie wchodzą do integrity_rejection_pct. Dlatego nawet bardzo wysoki udział starego GPS może współistnieć z flagą TRUE. Nie wolno interpretować tej flagi jako gwarancji świeżości całej floty, pełnego pokrycia źródła ani braku błędów ostatniego runu. Wskaźnik świeżości i ewentualne dodatkowe progi są kolejnym wymaganiem przed dashboardem. Helper is_hour_complete nie zastępuje tej reguły.

## 7. Zależności i interpretacja agregatów

- Sieciowe active_vehicles = active_bus_vehicles + active_tram_vehicles = liczba wierszy vehicle_activity_hourly.
- Sieciowe position_events = suma position_events po pojazdach = suma po liniach = silver_records.
- Zmiana linii przez pojazd może zwiększać sumę pojazdów po liniach względem liczby pojazdów sieci.
- Nie sumować godzinowych distinctów jako dziennych unikalnych pojazdów. Nie utożsamiać observation span z czasem pracy.
- Dzień lokalny może mieć 23/25 godzin. Klucze i łączenia pozostają UTC; lokalna prezentacja powtórzonej godziny musi zachować offset.
- Brak obserwacji linii/pojazdu nie dowodzi braku kursowania. Brak godziny lub jakości to luka/unknown, nie zero.
- Godziny provisional/incomplete/unknown trzeba pokazać jawnie; filtr bezpiecznych wyników nie może usuwać ich z osi bez śladu.
- Nie są wyliczane kilometry, punktualność, opóźnienia względem rozkładu ani pełna mapa live. To wymaga dodatkowych źródeł/reguł.

## 8. Korekty, niezmienność i zgodność

Naprawa sprawdza domyślnie ostatnie 24 zamknięte godziny. Zmiana listy wejść lub brak wymaganych plików Silver kieruje godzinę do przebudowy. Zmiana/usunięcie wcześniej użytego obiektu Bronze blokuje przebudowę do odtworzenia źródła. Gold jest naprawiany bez przebudowy aktualnego Silver przy brakującym/nieaktualnym commicie, złym zestawie tabel/URI, braku wskazanego pliku lub wymaganej finalizacji. Mutowalne legacy wejścia mają kontrolę tożsamości przed/po odczycie.

To logiczna odtwarzalność wyniku, nie identyczność bajtów: UUID i pipeline_run_at mogą się zmienić. Obiekty źródłowe i opublikowane wersje traktowane są jako niezmienne. Snapshot metadanych nie jest transakcyjnym odczytem wersji każdego obiektu; pool serializuje tylko współpracujących writerów Airflow. Ręczne zapisy wymagają tej samej koordynacji.

Catchup jest wyłączony. Historia poza oknem wymaga kontrolowanego backfillu. Retencja, trwały rejestr początku kolekcji, obsługa pełnej osi godzin i wersjonowanie schematu manifestu Gold pozostają zadaniami rozwoju. Zmianę kontraktu należy rozpatrywać wraz z migracją producenta i konsumenta; schema_version=2 opisuje manifest ingestii, nie wersję wszystkich tabel Gold.

## 9. Weryfikacja kontraktu

Testy: [test_bronze_to_silver.py](../tests/test_bronze_to_silver.py), [test_silver_to_gold.py](../tests/test_silver_to_gold.py), [test_late_data.py](../tests/test_late_data.py), [test_review_regressions.py](../tests/test_review_regressions.py). Scenariusze storage i awarii: [pipeline_e2e.py](../tests/pipeline_e2e.py). Instrukcja uruchomienia bez klucza API: [README PL](../README.pl.md#demo-bez-klucza-api-ztm).

Testy sprawdzają schematy/klucze, uzgodnienie liczników, kompletność, odrzucenia, granice publikacji, naprawy i finalizację. Nie dowodzą pełnego pokrycia API, ciągłej dostępności, gotowości backupu ani wydajności przy dowolnej skali.
