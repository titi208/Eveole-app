import pandas as pd
import json
import os
from datetime import datetime

DATA_DIR = "data"
OUTPUT_FILE = "static/db.json"

print("🚀 Conversion des données GTFS vers JSON Mobile...")

try:
    # 1. Chargement des fichiers
    print("   📂 Lecture des fichiers...")
    routes = pd.read_csv(f"{DATA_DIR}/routes.txt", dtype=str)[['route_id', 'route_short_name', 'route_long_name', 'route_color', 'route_text_color']]
    trips = pd.read_csv(f"{DATA_DIR}/trips.txt", dtype=str)[['route_id', 'service_id', 'trip_id', 'trip_headsign']]
    stops = pd.read_csv(f"{DATA_DIR}/stops.txt", dtype=str)[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    stop_times = pd.read_csv(f"{DATA_DIR}/stop_times.txt", dtype={'trip_id': str, 'stop_id': str})[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    calendar = pd.read_csv(f"{DATA_DIR}/calendar.txt", dtype=str)

    # 2. Conversion Temps (HH:MM:SS -> Secondes)
    def t2s(t):
        try:
            h,m,s = map(int, t.split(':'))
            return h*3600 + m*60 + s
        except: return 0

    stop_times['arr_sec'] = stop_times['arrival_time'].apply(t2s)
    stop_times['dep_sec'] = stop_times['departure_time'].apply(t2s)
    
    # On ne garde que les colonnes utiles pour alléger le JSON
    stop_times = stop_times.sort_values(by=['trip_id', 'stop_sequence'])

    # 3. Structuration des données
    print("   ⚙️ Structuration...")
    
    # ROUTES: Dictionnaire pour accès rapide
    routes_dict = {}
    for _, r in routes.iterrows():
        routes_dict[r['route_id']] = {
            'n': r['route_short_name'],
            'l': r['route_long_name'],
            'c': r['route_color'],
            'tc': r['route_text_color']
        }

    # STOPS: Dictionnaire
    stops_dict = {}
    for _, s in stops.iterrows():
        stops_dict[s['stop_id']] = {
            'n': s['stop_name'],
            'lat': float(s['stop_lat']),
            'lon': float(s['stop_lon'])
        }

    # CALENDAR: Dictionnaire
    calendar_dict = {}
    for _, c in calendar.iterrows():
        calendar_dict[c['service_id']] = {
            'start': int(c['start_date']),
            'end': int(c['end_date']),
            'days': [
                int(c['monday']), int(c['tuesday']), int(c['wednesday']),
                int(c['thursday']), int(c['friday']), int(c['saturday']), int(c['sunday'])
            ]
        }

    # TRIPS: On regroupe tout (C'est le plus gros morceau)
    # On fusionne trips et stop_times pour créer des objets complets
    # Structure: [route_id, service_id, headsign, [seq_stops]]
    
    trips_list = []
    
    # Grouper stop_times par trip_id
    print("   🔗 Association des horaires (Patientez)...")
    stops_group = stop_times.groupby('trip_id')
    
    for _, trip in trips.iterrows():
        tid = trip['trip_id']
        if tid in stops_group.groups:
            st_data = stops_group.get_group(tid)
            # On crée une liste légère pour les arrêts: [stop_id, arr_sec, dep_sec]
            schedule = st_data[['stop_id', 'arr_sec', 'dep_sec']].values.tolist()
            
            trips_list.append({
                'id': tid,
                'rid': trip['route_id'],
                'sid': trip['service_id'],
                'head': trip['trip_headsign'],
                'sch': schedule
            })

    # 4. Export JSON
    final_db = {
        'routes': routes_dict,
        'stops': stops_dict,
        'calendar': calendar_dict,
        'trips': trips_list
    }

    if not os.path.exists("static"): os.makedirs("static")
    
    print(f"   💾 Sauvegarde dans {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_db, f, separators=(',', ':')) # Minification

    print("✅ Terminé ! Le fichier static/db.json est prêt.")

except Exception as e:
    print(f"❌ Erreur : {e}")