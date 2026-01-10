# AIA Project (Weather Data Pipeline)

Proiect Python modular care:
1) face request către Open‑Meteo (Archive API),
2) parsează răspunsul într-un format tabelar (pandas DataFrame),
3) salvează datele brute ("dirty") și datele curățate ("clean"),
4) aplică pași de curățare (tipuri, duplicate, valori lipsă/outliers),
5) generează fișiere pentru analiză (statistici, corelații etc. — dacă sunt implementate în cod).

Scopul e să ai un pipeline simplu de tip **request → tabel → cache → clean → analyze** pe care poți construi ușor.

---

## Structura proiectului

- `src/` — codul sursă
  - `main.py` — entry point / demo CLI
  - `command.py` — orchestrare comenzi (save dirty, save clean, analyze etc.)
  - `api_request.py` — request către Open‑Meteo (cu cache și retry)
  - `openmeteo_parser.py` — parser: response → tabel (DataFrame / rows)
  - `data_cleaner.py` — curățare tabelară
  - `save_data.py` — salvare în cache (ex: xlsx / sqlite — depinde de implementarea din repo)
  - `data_analysis.py` — analiză (ex: summary statistics, correlation)

- `cached_data/` — cache în fișiere (după cum e în repo)
  - `dirty_data/` — date brute exportate
  - `cleaned_data/` — date curățate exportate
  - `analyzed_data/` — output de analiză

- `src/cache.sqlite` — cache SQLite (dacă este folosit de implementarea curentă pentru a păstra datele brute ca tabel)

---

## Cerințe

- Python 3.11+ recomandat
- Dependențe în `requirements.txt`

---

## Instalare (Windows PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Rulare

### 1) Rulare simplă (demo)

```powershell
python src\main.py
```

### 2) Rulare cu lat/lon

```powershell
python src\main.py 47.0269 28.8416
```

### 3) Rulare cu interval de timp

```powershell
python src\main.py 47.0269 28.8416 2015-12-01 2025-12-01
```

---

## Ce se salvează și unde (cache)

Proiectul folosește două tipuri de „cache” (în funcție de implementarea actuală din cod):

1) **Cache în fișiere** în `cached_data/`:
   - `cached_data/dirty_data/` — export cu date brute (ex: `weather_data.xlsx`)
   - `cached_data/cleaned_data/` — export cu date curățate (ex: `weather_clean_data.xlsx`)
   - `cached_data/analyzed_data/` — export pentru analiza datelor (ex: `summary_statistics.xlsx`, `corr_matrix.xlsx`)

2) **Cache în SQLite** (fișier local):
   - `src/cache.sqlite`

În mod ideal, după request:
- **response-ul** este transformat într-un tabel cu header (coloane) + rânduri (observații)
- apoi se face **insert** în SQLite într-un tabel (ex. `weather_dirty`), ca să poți face ulterior operații pe date.

---

## Fluxul de date (dirty → clean → analyze)

1. `ApiRequest` face request (lat/lon + interval) către Open‑Meteo.
2. `OpenMeteoParser` convertește răspunsul într-un format tabelar.
3. Se salvează **dirty** (date brute):
   - în `cache.sqlite` (ca tabel) și/sau în `cached_data/dirty_data/` (ca fișier, ex. xlsx)
4. `DataCleaner.clean(...)` aplică curățarea:
   - conversii de tipuri
   - eliminare duplicate
   - valori lipsă (drop/replace)
   - eliminare coloane complet goale (toate NaN sau 0)
5. Se salvează **clean** (date curățate)
6. `DataAnalysis` (dacă este folosit) produce fișiere de analiză.

---

## Troubleshooting

### Eroare: fișierul "nu poate fi găsit" la read_excel

Dacă vezi o eroare de genul:

`... cached_data\dirty_data\weather_data.xlsx` nu poate fi găsit,

înseamnă una din situațiile:
- fișierul nu a fost încă generat (nu ai rulat pasul de „save dirty” înainte de „save clean”)
- construirea path-ului e greșită (join/dirname/relativ vs absolut)

Ce verifici rapid:
- există `cached_data/dirty_data/weather_data.xlsx` după ce rulezi comanda care salvează dirty?
- în cod, funcția care construiește path-ul folosește `os.path.join(...)` și bazează path-ul pe directorul proiectului, nu pe directorul curent.

---

## Dezvoltare

- Codul e gândit să rămână modular: request/parsing/cleaning/saving separate.
- Dacă vrei să schimbi partea de salvare în SQLite, caută logica în `save_data.py` și/sau `api_request.py`.

---

## License

Proiect educațional / experimental.
