import requests
import pandas as pd
import os

# -------------------------------
# 1️⃣ Définition des indicateurs
# -------------------------------
indicators = {
    "GDP": "NY.GDP.MKTP.CD",
    "Population": "SP.POP.TOTL",
    "GDP Growth": "NY.GDP.MKTP.KD.ZG",
    "Government Spending": "NE.CON.GOVT.ZS",
    "Urban Population": "SP.URB.TOTL",
    "Poverty": "SI.POV.DDAY"
}

# -------------------------------
# 2️⃣ Lecture des ISO codes depuis data_catastrophe.xlsx
# -------------------------------
df_cat = pd.read_excel("../Data/data_catastrophe.xlsx")

# Mapping ISO3 -> ISO2
iso3_to_iso2 = {
    'FRA':'FR', 'HRV':'HR', 'BIH':'BA', 'IRL':'IE', 'NLD':'NL', 'GBR':'GB',
    'ALB':'AL', 'ITA':'IT', 'GRC':'GR', 'SRB':'RS', 'MKD':'MK', 'ESP':'ES',
    'RUS':'RU', 'POL':'PL', 'CZE':'CZ', 'AUT':'AT', 'BGR':'BG', 'BLR':'BY',
    'LVA':'LV', 'SWE':'SE', 'EST':'EE', 'PRT':'PT', 'MDA':'MD', 'SVN':'SI',
    'HUN':'HU', 'NOR':'NO', 'CHE':'CH', 'ROU':'RO', 'BEL':'BE', 'DEU':'DE',
    'DNK':'DK', 'UKR':'UA', 'SVK':'SK', 'ISL':'IS', 'LUX':'LU', 'LTU':'LT',
    'MNE':'ME'
}

# Conversion ISO3 -> ISO2 et nettoyage
wb_countries = [iso3_to_iso2.get(c.strip().upper()) for c in df_cat["ISO"].dropna().unique()]
wb_countries = [c for c in wb_countries if c is not None]
countries_str = ";".join(wb_countries)
print(f"Countries to fetch: {wb_countries}")

# -------------------------------
# 3️⃣ Création du dossier de sortie
# -------------------------------
os.makedirs("../Data", exist_ok=True)

# -------------------------------
# 4️⃣ Collecte des données via l'API World Bank
# -------------------------------
all_data = []

for name, code in indicators.items():
    print(f"Fetching {name}...")
    page = 1
    
    while True:
        url = f"https://api.worldbank.org/v2/country/{countries_str}/indicator/{code}?format=json&per_page=20000&page={page}"
        
        try:
            response = requests.get(url)
            data = response.json()
        except Exception as e:
            print(f"Erreur API pour {name}: {e}")
            break
        
        if not isinstance(data, list) or len(data) < 2 or data[1] is None:
            break
        
        for item in data[1]:
            if item["value"] is not None:
                try:
                    year = int(item["date"])
                except:
                    continue
                if 2010 <= year <= 2024:
                    all_data.append({
                        "country": item["country"]["value"],
                        "year": year,
                        "indicator": name,
                        "value": item["value"]
                    })
        
        if page >= data[0]["pages"]:
            break
        page += 1

# -------------------------------
# 5️⃣ Conversion en DataFrame
# -------------------------------
df = pd.DataFrame(all_data)

# -------------------------------
# 6️⃣ Vérification avant pivot
# -------------------------------
if df.empty:
    print("⚠️ Aucun data récupéré pour ces pays !")
else:
    # -------------------------------
    # 7️⃣ Pivot table
    # -------------------------------
    df_pivot = df.pivot_table(
        index=["country", "year"],
        columns="indicator",
        values="value"
    ).reset_index()
    
    df_pivot = df_pivot.sort_values(by=["country", "year"])
    
    # -------------------------------
    # 8️⃣ Affichage
    # -------------------------------
    print(df_pivot.head())
    
    # -------------------------------
    # 9️⃣ Sauvegarde en CSV
    # -------------------------------
    output_path = "../Data/world_bank_data.csv"
    df_pivot.to_csv(output_path, index=False)
    print(f"Dataset saved in: {output_path}")