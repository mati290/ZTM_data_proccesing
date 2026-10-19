# Kontrakt danych Gold

Dokument opisuje implementację po commicie `45a2e61` oraz reguły dla jej
konsumentów. Znane luki walidacji są wymienione na końcu; wymagania przyszłego
dashboardu nie oznaczają, że dashboard jest już zaimplementowany.

## Cel i reguły wspólne

Gold odpowiada na pytania biznesowe. Nie kopiuje wierszy Silver; agreguje je
do metryk o jawnie określonym ziarnie. Wszystkie przedziały godzinowe są
godzinami UTC w formacie `[date + hour, date + hour + 1h)`.

Nowe dane Bronze i manifest cyklu są przypisywane do partycji godziny UTC,
w której rozpoczyna się slot cyklu pobierania. Cykl przekraczający granicę
godziny lub północ zachowuje tę samą partycję dla autobusów, tramwajów
i manifestu. Pole `_ingested_at` nadal zawiera rzeczywisty czas zapisu
danych. Godzina partycji nie oznacza godziny zdarzenia GPS i może różnić
się od godziny `_ingested_at`. Historyczne obiekty zapisane według czasu
zapisu pozostają w dotychczasowych partycjach.

Źródłem obserwacji jest plik Silver wskazany przez pole `artifacts.data`
w aktywnym manifeście
`s3://silver/vehicle_positions/date=YYYY-MM-DD/hour=HH/_active.json`.
Nowe publikacje zapisują dane, kwarantannę i marker kompletności pod
wersjonowaną ścieżką `s3://silver/_versions/date=.../hour=.../<publication_id>/`.
Gdy aktywnego manifestu nie ma, odczyt zachowuje zgodność z historyczną
ścieżką `s3://silver/vehicle_positions/date=.../hour=.../data.parquet`.
Plik danych zawiera poprawne, deduplikowane obserwacje. Wiersz jest unikalny
w obrębie godziny dla `(vehicle_type, vehicle_number, gps_time_utc)`. Typ pojazdu
identyfikuje `bus` albo `tram`. Liczba pojazdów zawsze liczy pary
`(vehicle_type, vehicle_number)`, aby jednakowe numery autobusów i tramwajów
nie zostały zlane.

Marker kompletności wskazany przez `artifacts.completeness` tej samej
publikacji Silver jest źródłem `data_completeness_status`. Dla historycznego
układu plików jest to `_completeness.json` obok `data.parquet`:

- `complete` wymaga co najmniej 95% pokrycia oczekiwanych slotów UTC
  oraz co najmniej 90% kompletnych slotów spośród obecnych poprawnych slotów.
- `incomplete` oznacza obecność manifestów bez spełnienia obu progów.
- `unknown` oznacza brak manifestów albo brak markera kompletności.

Slot jest kompletny, jeśli przynajmniej jeden jego manifest ma status
`complete`. Duplikaty nie zwiększają pokrycia ani udziału kompletnych slotów.
Sloty błędne, spoza godziny, z przyszłości lub niewyrównane do interwału
nie zwiększają pokrycia. Przy mieszanych manifestach starsze wpisy bez slotu
nie uzupełniają brakujących slotów. Obecne pole `cycle_slot_start` z błędną
wartością, w tym `null`, nie uruchamia historycznego sposobu liczenia.

Wyłącznie godziny, w których żaden manifest nie zawiera pola
`cycle_slot_start`, korzystają z historycznego liczenia manifestów:
co najmniej 95% oczekiwanej liczby cykli i co najmniej 90% manifestów
ze statusem `complete`. Ich metryki unikalnych slotów pozostają nieznane.

Dla zakończonej godziny i interwału 30 sekund oczekiwane jest 120 slotów,
więc minimum pokrycia wynosi 114. Podczas oceny bieżącej godziny liczba
oczekiwanych slotów obejmuje tylko sloty rozpoczęte do czasu oceny włącznie.
Zadania godzinowe Airflow przetwarzają zakończone godziny.

`complete` opisuje spełnienie progów pobierania, a nie gwarancję obserwacji
wszystkich pojazdów. `unknown` nigdy nie jest traktowane jako `complete`.
Błędy odczytu storage oraz wykryte błędy statusu, typów lub zakresów
liczników przerywają budowanie Gold zamiast zwracać `unknown`. Aktualna
walidacja nie obejmuje jednak wszystkich sprzeczności statusu z proporcjami
slotów; ograniczenie to opisano na końcu.

Wiersze z niekompletnych lub nieznanych godzin można zachować w Gold razem ze
statusem, ale dashboardy nie mogą przedstawiać ich jako wiarygodnych wartości
bez oznaczenia. Widoki prezentujące wyłącznie wiarygodne metryki filtrują
`hourly_data_quality.safe_for_dashboard = TRUE`, łącząc tabele dla tej samej
godziny i publikacji Gold. Sam status `complete` nie potwierdza finalizacji
ani spełnienia reguł jakości rekordów. Brak partycji Gold nie oznacza zera.

## Wspólne kolumny wszystkich tabel

Poniższe kolumny występują we wszystkich czterech tabelach; definicje dotyczą
również publikacji diagnostycznej `hourly_data_quality`.

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `date` | `DATE` | Data UTC partycji cyklu pobierania |
| `hour` | `TINYINT` | Godzina UTC partycji, zakres `0..23` |
| `data_completeness_status` | `VARCHAR` | `complete`, `incomplete` albo `unknown` z markera Silver po dostępnych walidacjach |
| `is_final` | `BOOLEAN` | Czas budowania wynosi co najmniej koniec godziny + 15 min; nie zamyka okna korekt |
| `source_max_ingested_at` | `TIMESTAMPTZ` | Największy `ingested_at` użytego Silver; `NULL` przy braku lub pustym Silver; nie identyfikuje wersji |
| `pipeline_run_at` | `TIMESTAMPTZ` | Wspólny czas UTC wykonania budowy Gold dla godziny |

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
| Źródło | Silver, kwarantanna i marker kompletności tej samej publikacji Silver; liczniki cykli i slotów pochodzą z markera, nie z ponownego listowania Bronze przez Gold |
| Warunek publikacji | W pełnej publikacji jest zatwierdzany wspólnym manifestem razem z trzema tabelami metryk. Gdy Silver jest brakujący lub pusty, publikowana jest tylko diagnostyka jakości, po czym zadanie kończy się błędem |
| Brak danych | Wygenerować rekord jakości z jawnym `silver_partition_status`; nie zastępować nieznanych liczników zerami |
| Niekompletność | Zachować dostępne liczniki i status kompletności. Status zadania pozostaje w Airflow, poza schematem tej tabeli. Nie nazywać takich metryk wiarygodną aktywnością sieci |

| Kolumna | Definicja weryfikowalna w SQL |
|---|---|
| `date`, `hour` | Partycja UTC godziny (`data_interval_start` runu Airflow) |
| `silver_partition_status` | `present`, gdy wskazany plik danych Silver istnieje (również gdy jest pusty); `missing`, gdy go brak. Pusta partycja jest dodatkowo oznaczana jako `silver_empty` w manifeście diagnostycznym |
| `data_completeness_status` | Zweryfikowany status markera kompletności Silver; `unknown`, gdy markera brak lub jawnie raportuje brak manifestów. Błędy odczytu i walidacji przerywają zadanie |
| `manifest_cycles` | Liczba manifestów uwzględnionych w markerze kompletności publikacji Silver; obejmuje duplikaty; `NULL`, gdy licznik jest nieznany |
| `complete_manifest_cycles` | Liczba tych manifestów z `status = 'complete'` |
| `complete_cycle_ratio` | `complete_manifest_cycles / manifest_cycles`; `NULL` przy nieznanych licznikach lub zerowym mianowniku. Opisuje manifesty; dla danych ze slotami nie jest proporcją decydującą o kompletności |
| `partial_manifest_cycles` | `manifest_cycles - complete_manifest_cycles`; `NULL`, gdy liczba cykli jest nieznana |
| `expected_slots` | Oczekiwana liczba slotów, odczytana z `expected_cycles` markera Silver; 120 dla pełnej godziny przy interwale 30 s; `NULL`, gdy brak tej informacji |
| `observed_slots` | Liczba unikalnych poprawnych slotów UTC uwzględnionych w ocenie; `0`, gdy metadane slotów istnieją, ale żaden slot nie jest poprawny; `NULL` dla historycznych manifestów bez slotów |
| `complete_slots` | Liczba kompletnych unikalnych slotów; `0` przy braku kompletnych slotów, `NULL`, gdy liczba jest nieznana |
| `slot_coverage_ratio` | `observed_slots / expected_slots`; `NULL` przy nieznanych licznikach lub zerowym mianowniku |
| `complete_slot_ratio` | `complete_slots / observed_slots`; `NULL` przy nieznanych licznikach lub zerowym mianowniku |
| `bronze_records` | `silver_position_events + quarantined_records` (zastępuje `records_processed`). Jest to dokładna liczba rekordów Bronze po odczycie, bo transformacja Bronze→Silver weryfikuje `przed - po = odrzucone`; `NULL`, gdy którykolwiek składnik jest nieznany |
| `silver_position_events` | `COUNT(*)` z Silver; `NULL`, gdy partycja Silver nie istnieje |
| `quarantined_records` | `COUNT(*)` z kwarantanny aktywnej publikacji Silver; `NULL`, gdy pliku brak. Błąd dostępu lub odczytu przerywa zadanie |
| `rejection_pct` | `100 * quarantined_records / bronze_records` (skala 0–100); `NULL`, gdy `bronze_records` jest `NULL` lub 0. Zawiera również odrzucenia rutynowe (`duplicate`, `stale_gps_time`), więc nie jest samodzielnym progiem jakości |
| `duplicate_records` | Liczba wierszy kwarantanny z `rejection_reason = 'duplicate'`; `NULL`, gdy brak kwarantanny |
| `integrity_rejection_pct` | Jak `rejection_pct`, ale bez przyczyn rutynowych (`duplicate`, `stale_gps_time`), tj. `invalid_coordinates`, `future_gps_time` itp. |
| `quarantined_by_reason` | Liczności `GROUP BY rejection_reason` z kwarantanny; pusta lista tylko wtedy, gdy kwarantanna została odczytana i nie zawiera wierszy |
| `safe_for_dashboard` | `TRUE` wtedy i tylko wtedy, gdy `data_completeness_status = 'complete'`, Silver ma co najmniej jeden wiersz, `is_final = TRUE`, `bronze_records` jest znane i `integrity_rejection_pct <= 5`. Nigdy `NULL` (brak informacji = `FALSE`) |
| `is_final` | `TRUE`, gdy czas budowania publikacji jest nie wcześniejszy niż koniec godziny + 15 min; nie zamyka 24-godzinnego okna korekt |
| `source_max_ingested_at` | `MAX(ingested_at)` z użytego Silver; `NULL`, gdy Silver nie istnieje lub jest pusty; nie jest identyfikatorem wersji ani pełnym znacznikiem zmian wejścia |
| `pipeline_run_at` | Czas UTC utworzenia wiersza jakości |

Sama lista partycji Bronze, Silver i manifestów nie pozwala wykryć godziny,
w której ingestion całkowicie nie działał: dla takiej godziny nie ma obiektu do
odkrycia. Obecny Gold nie pobiera statusów Airflow ani nie generuje pełnej osi
oczekiwanych godzin. Zadania mogą publikować diagnostykę brakującego Silver,
ale nie gwarantuje to wiersza jakości dla każdej luki. Konsument musi połączyć
wyniki z oczekiwaną osią czasu, a stan wykonania sprawdzać w orkiestracji.
Brak wiersza jakości oznacza „brak informacji”, a nie zero pojazdów ani
brak aktywności.

## Reguły interpretacji dashboardu

Są to wymagania dla przyszłego konsumenta; dashboard nie jest jeszcze
częścią zaimplementowanego projektu.

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
   gotowa według okresu oczekiwania (`is_final = TRUE`), gdy czas runu
   ≥ koniec godziny + 15 min (`FINALITY_GRACE`); wcześniej flaga ma wartość
   `FALSE`. Okno dopuszczalnych korekt jest osobną regułą.
   `safe_for_dashboard` wymaga dodatkowo `is_final = TRUE`.
   `source_max_ingested_at` to największy `ingested_at` w Silver użytym do
   budowy partycji (`NULL`, gdy Silver nie istnieje lub jest pusty). Sama
   wartość tego maksimum nie identyfikuje wszystkich zmian wejścia ani wersji
   Silver; do tego służą `source_inputs` i `source_silver_uri`.
   `is_final` oznacza gotowość po okresie oczekiwania, a nie niezmienność
   danych. Korekty nadal są dopuszczane w 24-godzinnym oknie.
   Manifest pełnej publikacji Gold zawiera `is_final` oraz `source_silver_uri`.
   DAG korekt wybiera również godziny wymagające finalizacji, nawet gdy nie
   przybyło danych Bronze, i aktualizuje sam Gold, zachowując aktualny Silver.
   Finalizacja następuje podczas pierwszego wykonania DAG-u po upływie okresu
   oczekiwania; przy harmonogramie `:30` nie musi nastąpić dokładnie w `:15`.
7. Polityka korekt: DAG `reprocess_late_data_hourly` (co godzinę, `:30`)
   sprawdza zamknięte godziny z ostatnich 24 h. Nowe publikacje Silver zapisują
   w aktywnym manifeście `source_inputs`: klucze, ETag, rozmiary i czasy
   modyfikacji wejściowych plików Bronze oraz manifestów cykli. Zmiana tego
   zestawu powoduje przebudowę Silver i Gold, również gdy nowe obiekty
   powstały przed publikacją Silver. Dane wybranych plików są odczytywane raz
   do tabeli tymczasowej DuckDB, wspólnej dla kontroli jakości, Silver
   i kwarantanny. Kompletność korzysta z wybranej listy manifestów.
   Bronze jest traktowane jako wejście append-only: istniejące obiekty nie
   powinny być nadpisywane. Listowanie metadanych nie jest transakcyjnym
   snapshotem storage, a ETag nie jest zakładany jako suma MD5.
   Dla starszych publikacji bez `source_inputs` pozostaje porównanie czasów
   modyfikacji. Brakujący, nieaktualny lub wymagający finalizacji Gold jest
   odbudowywany bez przebudowy aktualnego Silver. Starsze godziny poza oknem
   korekt nie są automatycznie przeliczane. Wspólny pool Airflow serializuje
   zapisy projektowych DAG-ów; uruchomienia ręczne muszą przestrzegać tej
   samej zasady.

## Implementacja (`silver_to_gold.py`)

- **Publikacja atomowa:** tabele `vehicle_activity_hourly`, `hourly_line_metrics`, `hourly_network_metrics` i `hourly_data_quality` są najpierw zapisywane w `s3://gold/_staging/`, a następnie walidowane pod kątem schematu i liczby wierszy. Po walidacji pliki trafiają do unikalnych, wersjonowanych ścieżek w `s3://gold/_versions/`. Dopiero po przygotowaniu wszystkich czterech tabel zapisywany jest manifest w `s3://gold/_commits/`, wskazujący URI opublikowanych plików i ich liczby wierszy. Konsument powinien odczytywać manifest i korzystać z URI, które wskazuje, zamiast zakładać stałe ścieżki plików.
- **Granica publikacji:** manifest pod `s3://gold/_commits/date=YYYY-MM-DD/hour=HH/manifest.json` jest punktem zatwierdzenia pełnej publikacji Gold. Zawiera `publication_id`, `partition`, `source_silver_uri`, `is_final` oraz URI i liczby wierszy wszystkich tabel. Konsument powinien odczytać go raz i używać wskazanych URI dla całej publikacji. Jeśli przygotowanie lub kopiowanie tabel zakończy się błędem przed zapisaniem manifestu, poprzednia publikacja pozostaje wskazywana przez dotychczasowy manifest. Zapis pojedynczego obiektu manifestu w S3 atomowo zastępuje jego poprzednią wersję.
- **Publikacja diagnostyczna:** gdy Silver nie istnieje lub jest pusty, pipeline publikuje wyłącznie `hourly_data_quality` oraz osobny manifest diagnostyczny w `s3://gold/_diagnostics/date=YYYY-MM-DD/hour=HH/manifest.json`. Pole `status` przyjmuje wartość `silver_missing` dla brakującej partycji albo `silver_empty` dla istniejącej, lecz pustej partycji. Manifest diagnostyczny zawiera `publication_id`, `partition`, `status` oraz URI i liczbę wierszy tabeli `hourly_data_quality`. Nie jest to pełna publikacja Gold i manifest diagnostyczny nie zastępuje ani nie nadpisuje manifestu pełnej publikacji w `s3://gold/_commits/date=YYYY-MM-DD/hour=HH/manifest.json`. Konsumenci szukający diagnostyki powinni sprawdzać ścieżkę `_diagnostics/`; konsumenci pełnych danych Gold powinni korzystać wyłącznie z `_commits/`.
- **Idempotencja:** ponowne przeliczenie godziny publikuje nową wersję plików i aktualizuje manifest. `pipeline_run_at` jest metadaną uruchomienia i może się zmieniać; pozostałe dane powinny być zgodne z wejściem i kontraktem.
- **Walidacje:** schemat i zawartość Silver przed agregacją, schemat i liczba wierszy plików staging, spójność krzyżowa tabel oraz unikalność kluczy. Liczba kompletnych slotów nie może przekraczać liczby obecnych slotów ani manifestów complete; liczba obecnych slotów nie może przekraczać oczekiwanych slotów ani znanej liczby manifestów. Liczniki muszą być nieujemnymi liczbami całkowitymi; wartości logiczne są odrzucane. Proporcje Gold są wyliczane z liczników.
- **Kompletność:** godziny ze statusem `incomplete` lub `unknown` są publikowane z jawnym `data_completeness_status`. Konsument powinien uwzględniać ten status oraz `safe_for_dashboard` i `is_final`, zamiast traktować samą obecność danych jako gwarancję ich gotowości.
- **Zakres jakości:** jakość nie obejmuje statusu Airflow. Luki w godzinach należy wykrywać na podstawie orkiestracji i oczekiwanej osi czasu na dashboardzie.

## Znane ograniczenia implementacji i planowane uzupełnienia

Poniższe ograniczenia dotyczą stanu po `45a2e61`; nie są opisem już
zaimplementowanych zabezpieczeń.

- **Spójność statusu z licznikami:** `load_completeness` sprawdza typy,
  zakresy i relacje liczników, lecz ufa statusowi markera bez ponownej oceny
  progów 95%/90%. Marker `complete` z 1/120 slotów może przejść walidację,
  a `safe_for_dashboard` może wtedy przyjąć `TRUE`. Dla prawidłowego markera
  producent stosuje opisane progi; odrzucanie sprzecznych markerów jest
  następną poprawką.
- **Kontrola publikacji Gold:** wybór napraw sprawdza partycję manifestu,
  `source_silver_uri` i finalizację, ale jeszcze nie kompletny zestaw tabel
  ani istnienie wszystkich wskazanych plików. Zachowany manifest nie dowodzi,
  że publikacja nadal jest czytelna po ręcznym usunięciu obiektu.
- **Legacy:** stałe URI historycznego Silver nie wykrywa nadpisania jego
  zawartości. Pełna tożsamość wersji dotyczy nowych publikacji wersjonowanych.
- **Metadane i wejście:** brak wersji schematu manifestów oraz pełnej
  walidacji elementów `source_inputs`, spójności pól manifestu cyklu
  i powodów kwarantanny. Odczyt Bronze korzysta z wykrywania schematu JSON;
  brak kolumn może zatrzymać transformację przed agregacją Gold.
- **Interwał:** opisane liczby slotów dotyczą 30 s. Dowolne inne dodatnie
  interwały są akceptowane przez konfigurację, lecz nie wszystkie odpowiadają
  wzorowi oczekiwanych slotów. Manifesty nie utrwalają użytego interwału.
- **Równoległość:** pool serializuje projektowe DAG-i, nie niezależne
  uruchomienia ręczne. Gold rozwiązuje dane i marker w osobnych odczytach;
  przełączenie aktywnej wersji przez innego writera może je rozdzielić.
- **Naprawy i utrzymanie:** brak wszystkich danych Bronze nie uruchamia
  porównania snapshotu; błąd podczas selekcji godzin może przerwać cały skan.
  Automatyczne naprawy nie obejmują historii poza oknem 24 h. Retencja
  wersji i osieroconych plików oraz kompletny runbook backfill pozostają
  do wdrożenia. Nie usuwać wersji bez sprawdzenia referencji manifestów.
