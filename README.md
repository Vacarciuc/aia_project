# AIA Project (Weather + PV Power Pipeline)

Acest proiect folosește atât date meteo (Open‑Meteo), cât și date de producție fotovoltaică (PV), pentru a construi un pipeline end‑to‑end: selectarea contorului → extragerea metadatelor → request meteo → curățare → analiză → feature engineering → modele → train/test.

---

## Date necesare (PV)

Ai nevoie de:
- fișier de **metadate** (de ex. info contor, coordonate, interval de funcționare),
- fișier cu **date de producere** (seria de putere/energie).

Aceste fișiere au fost descărcate de la: Photovoltaic Power Production Dataset — Mendeley Data  
https://data.mendeley.com/datasets/dbh93b6vp8/3

Și au fost salvate în:
- `cached_data/power_plant/`
  - ex: `cached_data/power_plant/metadata.csv`
  - ex: `cached_data/power_plant/production.csv` 

---

## Structura proiectului

- `src/` — codul sursă
  - `main.py` — entry point / CLI: selectezi contorul pentru care vrei predicția; automat extrage coordonatele + data start/sfârșit din metadate; face request la Open‑Meteo pentru intervalul respectiv
  - `command.py` — orchestrare comenzi (save dirty, save clean, analyze, train/test)
  - `api_request.py` — request către Open‑Meteo (cu cache și retry)
  - `openmeteo_parser.py` — parser: response → tabel (DataFrame / rows)
  - `data_cleaner.py` — curățare tabelară (mixer meteo + PV)
  - `save_data.py` — salvare în cache (xlsx/sqlite)
  - `data_analysis.py` — analiză (statistici, corelații, grafice)
  - `feature_engineering.py` — generare feature‑uri (laguri, rolling, meteo‑derived)
  - `models.py` — modele ML (de exemplu: LinearRegression, RandomForest, XGBoost)
  - `train_test.py` — split temporal, cross‑validation, scoruri

- `cached_data/` — cache în fișiere
  - `dirty_data/` — date brute (meteo + PV înainte de curățare)
  - `cleaned_data/` — date curățate și sincronizate
  - `analyzed_data/` — rezultate de analiză (summary, corelații)
  - `power_plant/` — datele PV descărcate (metadata + production)


---

## Instalare (Windows PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Rulare

### 1) Selectarea contorului și rularea pipeline‑ului
În `main.py` alegi contorul (ID/număr). Scriptul:
- citește `cached_data/power_plant/metadata.*`,
- extrage automat coordonatele (lat/lon) + data de start/sfârșit pentru acel contor,
- face request la Open‑Meteo (Archive API) pentru intervalul selectat,
- aliniază datele meteo cu producția PV pentru acel contor.



---

## Fluxul de date (PV + Meteo → dirty → clean → analyze → features → models)

1. Selectarea contorului:
   - din `metadata` se iau: `meter_id`, `latitude`, `longitude`, `start_date`, `end_date`, alte atribute (orientare, capacitate dacă există).
2. Open‑Meteo request:
   - `ApiRequest` cere variabile meteo relevante (radiație, temperatură, vânt, nori etc.) pentru intervalul `[start_date, end_date]` și coordonatele contorului.
   - `OpenMeteoParser` convertește răspunsul într-un DataFrame (timp + variabile).
3. PV production ingest:
   - `production.csv` (sau fișiere per contor) se încarcă, se filtrează după `meter_id`, se parsează timestamp‑urile și unitățile.
4. Salvare dirty:
   - se salvează datele brute meteo și PV în `cached_data/dirty_data/` și/sau în `src/cache.sqlite` (tabele `pv_dirty`, `weather_dirty`).
5. Curățare:
   - conversii de tipuri, uniformizare timestamp la aceeași granularitate (ex: orar),
   - eliminare duplicate,
   - tratare valori lipsă (drop/impute),
   - eliminarea coloanelor complet goale,
   - sincronizare temporală meteo ↔ PV (inner/left join pe timp).
6. Salvare clean:
   - setul curățat (joined meteo+PV) se exportă în `cached_data/cleaned_data/` și/sau în SQLite (`pv_weather_clean`).
7. Analiză:
   - `DataAnalysis` produce: summary statistics, distribuții, corelații (ex: Pearson între radiație și putere), grafice de sezonalitate.
   - export în `cached_data/analyzed_data/` (ex: `summary_statistics.xlsx`, `corr_matrix.xlsx`).
8. Feature engineering:
   - laguri ale producției (t-1, t-24),
   - feronțe mobile (rolling mean/max),
   - transformări meteo (clear‑sky index dacă e disponibil, interacțiuni),
   - encoding pentru calendaring (ora din zi, zi din săptămână, lună, sărbători).
9. Modele:
   - modele baseline: LinearRegression,
   - tree‑based: RandomForestRegressor, XGBoost,
   - opțional: modele temporale (SARIMAX) sau ML cu validare temporală.
10. Train/Test Split:
   - split temporal (ex: 80% train, 20% test la finalul intervalului),
   - opțional cross‑validation de tip TimeSeriesSplit,
   - metrice: MAE, RMSE, MAPE.
11. Salvare modele și scoruri:
   - modele antrenate se salvează (pickle/joblib) în `cached_data/analyzed_data/models/`,
   - scorurile și diagramele de eroare se exportă alături de predicții.

---

## Cerințe

- Python 3.11+ recomandat
- Dependențe în `requirements.txt`

---

## Troubleshooting

- Dacă nu se găsește `metadata` sau `production` în `cached_data/power_plant/`, verifică numele fișierelor și formatul (csv/xlsx).
- Asigură-te că `meter_id` din `production` există în `metadata`.
- Pentru erori la request Open‑Meteo, verifică intervalul (`start_date`, `end_date`) să fie valid și coordonatele să fie numerice.

---

## License

Proiect educațional / experimental.
