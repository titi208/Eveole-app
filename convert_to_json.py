import pandas as pd
import json
import os

DATA_DIR = "data"
OUTPUT_FILE = "static/db.json"

print("🚀 Optimisation Mobile V2 (Performance & Relations)...")

try:
    # 1. Chargement
    print("   📂 Lecture des fichiers...")
    routes = pd.read_csv(f"{DATA_DIR}/routes.txt", dtype=str)[['route_id', 'route_short_name', 'route_long_name', 'route_color', 'route_text_color']]
    trips = pd.read_csv(f"{DATA_DIR}/trips.txt", dtype=str)[['route_id', 'service_id', 'trip_id', 'trip_headsign']]
    stops = pd.read_csv(f"{DATA_DIR}/stops.txt", dtype=str)[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    stop_times = pd.read_csv(f"{DATA_DIR}/stop_times.txt", dtype={'trip_id': str, 'stop_id': str})[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    calendar = pd.read_csv(f"{DATA_DIR}/calendar.txt", dtype=str)

    # 2. Conversion Temps
    def t2s(t):
        try: h,m,s = map(int, t.split(':')); return h*3600 + m*60 + s
        except: return 0

    stop_times['arr_sec'] = stop_times['arrival_time'].apply(t2s)
    stop_times['dep_sec'] = stop_times['departure_time'].apply(t2s)
    stop_times = stop_times.sort_values(by=['trip_id', 'stop_sequence'])

    # 3. Pré-calcul des relations (Pour éviter le lag en JS)
    print("   🧠 Calcul des relations Arrêts <-> Lignes...")
    
    # On associe chaque trip à sa ligne pour savoir quelles lignes passent à quel arrêt
    merged = stop_times.merge(trips[['trip_id', 'route_id']], on='trip_id')
    
    # Dictionnaire : stop_id -> liste des route_ids qui y passent
    stop_to_routes = merged.groupby('stop_id')['route_id'].unique().apply(list).to_dict()
    
    # Dictionnaire : route_id -> liste des stop_ids (pour afficher les arrêts d'une ligne)
    route_to_stops = merged.groupby('route_id')['stop_id'].unique().apply(list).to_dict()

    # 4. Construction DB
    print("   ⚙️ Structuration JSON...")
    
    routes_dict = {}
    for _, r in routes.iterrows():
        rid = r['route_id']
        routes_dict[rid] = {
            'n': r['route_short_name'],
            'l': r['route_long_name'],
            'c': r['route_color'],
            'tc': r['route_text_color'],
            'stops': route_to_stops.get(rid, []) # Liste des arrêts de cette ligne
        }

    stops_dict = {}
    for _, s in stops.iterrows():
        sid = s['stop_id']
        stops_dict[sid] = {
            'n': s['stop_name'],
            'lat': float(s['stop_lat']),
            'lon': float(s['stop_lon']),
            'r': stop_to_routes.get(sid, []) # Liste des lignes qui passent ici
        }

    calendar_dict = {}
    for _, c in calendar.iterrows():
        calendar_dict[c['service_id']] = {
            's': int(c['start_date']),
            'e': int(c['end_date']),
            'd': [int(c['monday']), int(c['tuesday']), int(c['wednesday']), int(c['thursday']), int(c['friday']), int(c['saturday']), int(c['sunday'])]
        }

    trips_list = []
    stops_group = stop_times.groupby('trip_id')
    
    print("   🔗 Compression des horaires...")
    for _, trip in trips.iterrows():
        tid = trip['trip_id']
        if tid in stops_group.groups:
            st_data = stops_group.get_group(tid)
            # Format ultra-compact : [stop_id, arrival_sec] (on retire dep_sec pour alléger, sauf si terminus)
            schedule = st_data[['stop_id', 'arr_sec']].values.tolist()
            trips_list.append({
                'id': tid,
                'rid': trip['route_id'],
                'sid': trip['service_id'],
                'head': trip['trip_headsign'],
                'sch': schedule
            })

    final_db = {
        'routes': routes_dict,
        'stops': stops_dict,
        'calendar': calendar_dict,
        'trips': trips_list
    }

    if not os.path.exists("static"): os.makedirs("static")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_db, f, separators=(',', ':'))

    print("✅ Terminé ! DB optimisée générée.")

except Exception as e:
    print(f"❌ Erreur : {e}")