#!/usr/bin/env python3
"""
Скрипт для парсинга парковок Yandex Go (только cluster и cluster_empty).
Использует те же функции что и fetch_scooters.py, но фильтрует только парковки.

Использование:
    python3 fetch_parkings.py --bbox 39.6,43.4,39.9,43.7
"""

# Импортируем всё из fetch_scooters
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

# Импортируем функции из fetch_scooters
from fetch_scooters import (
    load_config, load_city_polygon, get_polygon_bbox,
    fetch_scooters, extract_points_from_response, simple_cluster_points,
    shrink_bbox_around_point
)

import json
import time
import argparse
from datetime import datetime

def extract_parkings_only(data):
    """Извлекает только парковки из ответа API."""
    parkings = []
    objects = data.get('objects', {})
    
    for obj_type in objects.get('objects_by_type', []):
        type_name = obj_type.get('type')
        if type_name in ['cluster', 'cluster_empty']:
            for obj in obj_type.get('objects', []):
                if isinstance(obj, dict):
                    parkings.append(obj)
    
    return parkings

def fetch_city_parkings(city_bbox, city_id, headers, delay=0.1):
    """Парсинг парковок города."""
    print(f"\n🅿️  Парсинг парковок города: {city_id}")
    print("="*80)
    
    center_lon = (city_bbox[0] + city_bbox[2]) / 2
    center_lat = (city_bbox[1] + city_bbox[3]) / 2
    user_location = [center_lon, center_lat]
    
    # Этап 1: Обзор
    print(f"\n📡 Этап 1: Обзорный запрос (zoom 12)")
    overview_data = fetch_scooters(city_bbox, user_location, zoom=12, headers=headers, delay=delay)
    
    if not overview_data:
        return {}
    
    all_points = extract_points_from_response(overview_data)
    print(f"   Найдено точек: {len(all_points)}")
    
    if len(all_points) == 0:
        return {}
    
    # Этап 2: Кластеризация
    print(f"\n🔥 Этап 2: Кластеризация (сетка 0.02°)")
    hot_zones = simple_cluster_points(all_points, grid_size_deg=0.02)
    print(f"   Горячих зон: {len(hot_zones)}")
    
    # Этап 3: Детальные запросы
    print(f"\n�� Этап 3: Детальные запросы (zoom 17)")
    
    all_parkings = {}
    
    for i, zone in enumerate(hot_zones, 1):
        zone_bbox = zone['bbox']
        zone_center = [
            (zone_bbox[0] + zone_bbox[2]) / 2,
            (zone_bbox[1] + zone_bbox[3]) / 2
        ]
        
        print(f"   [{i}/{len(hot_zones)}] Зона...", end=' ')
        
        detail_data = fetch_scooters(zone_bbox, zone_center, zoom=17, headers=headers, delay=delay)
        
        if not detail_data:
            print("⚠️")
            continue
        
        parkings = extract_parkings_only(detail_data)
        
        for parking in parkings:
            parking_id = parking.get('id')
            if parking_id:
                all_parkings[parking_id] = parking
        
        print(f"✓ {len(parkings)} парковок")
    
    return all_parkings

def save_geojson(parkings_dict, output_path, city_id):
    """Сохранение парковок в GeoJSON."""
    features = []
    stats = {'cluster': 0, 'cluster_empty': 0, 'total_scooters': 0}
    
    for obj_id, obj in parkings_dict.items():
        geo = obj.get('geo')
        if not geo:
            continue
        
        obj_type = obj_id.split('_')[0]
        properties = {"id": obj_id, "city_id": city_id, "type": obj_type}
        
        if obj_type == 'cluster':
            count = obj.get('payload', {}).get('objects_count', 0)
            properties["objects_count"] = count
            stats['cluster'] += 1
            stats['total_scooters'] += count
        else:
            stats['cluster_empty'] += 1
        
        features.append({
            "type": "Feature",
            "id": obj_id,
            "geometry": {"type": "Point", "coordinates": geo},
            "properties": properties
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "city_id": city_id,
            "generated_at": datetime.now().isoformat(),
            "parkings_with_scooters": stats['cluster'],
            "empty_parkings": stats['cluster_empty'],
            "total_scooters_on_parkings": stats['total_scooters']
        }
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    return stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('city_id', nargs='?')
    parser.add_argument('--bbox', type=str)
    parser.add_argument('--delay', type=float, default=0.1)
    args = parser.parse_args()
    
    headers = load_config()
    
    if args.bbox:
        parts = args.bbox.split(',')
        city_bbox = [float(x) for x in parts]
        city_id = f"custom_{int(time.time())}"
    elif args.city_id:
        city_feature = load_city_polygon(args.city_id)
        city_bbox = get_polygon_bbox(city_feature['geometry']['coordinates'])
        city_id = args.city_id
    else:
        print("❌ Укажите city_id или --bbox")
        sys.exit(1)
    
    start_time = time.time()
    parkings = fetch_city_parkings(city_bbox, city_id, headers, delay=args.delay)
    
    if not parkings:
        print("\n❌ Парковки не найдены")
        sys.exit(0)
    
    output_path = Path(__file__).parent / 'output' / 'parkings.geojson'
    stats = save_geojson(parkings, output_path, city_id)
    
    print("\n✅ ГОТОВО!")
    print(f"📄 {output_path}")
    print(f"⏱️  {time.time() - start_time:.1f} сек")
    print(f"\n📊 Парковок с самокатами: {stats['cluster']}")
    print(f"   Пустых парковок: {stats['cluster_empty']}")
    print(f"   Самокатов на парковках: {stats['total_scooters']}")

if __name__ == "__main__":
    main()
