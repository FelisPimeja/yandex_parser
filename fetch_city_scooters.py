#!/usr/bin/env python3
"""
Скрипт для полного парсинга самокатов в городе с использованием комбинированного подхода:
1. Стартовый запрос с низким zoom (обзор города)
2. Кластеризация точек в "горячие" зоны
3. Детальные запросы для горячих зон с высоким zoom
4. Рекурсивное раскрытие больших кластеров

Использование:
    python3 fetch_city_scooters.py polygon-184332  # По ID города из cities.geojson
    python3 fetch_city_scooters.py --bbox 39.6,43.4,39.9,43.7  # По custom bbox
"""

import json
import os
import sys
import argparse
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import requests

# Базовый URL API Yandex
BASE_URL = "https://tc.mobile.yandex.net"


def load_config():
    """Загрузка заголовков из config.json."""
    config_path = Path(__file__).parent / 'config.json'
    
    if not config_path.exists():
        print("❌ Ошибка: файл config.json не найден!")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    headers = config.get('headers') or config.get('yandex_headers')
    
    if not headers:
        print("❌ Ошибка: заголовки не найдены в config.json!")
        sys.exit(1)
    
    return headers


def load_city_polygon(city_id):
    """Загрузка полигона города из cities.geojson."""
    cities_path = Path(__file__).parent / 'output' / 'cities.geojson'
    
    if not cities_path.exists():
        print("❌ Ошибка: файл output/cities.geojson не найден!")
        print("Сначала запустите: python3 fetch_cities.py")
        sys.exit(1)
    
    with open(cities_path, 'r', encoding='utf-8') as f:
        cities = json.load(f)
    
    for feature in cities['features']:
        if feature['id'] == city_id:
            return feature
    
    print(f"❌ Город {city_id} не найден в cities.geojson")
    sys.exit(1)


def get_polygon_bbox(polygon_coords):
    """Вычисление bbox для полигона."""
    all_coords = []
    
    if isinstance(polygon_coords[0][0], list):
        # Polygon
        for ring in polygon_coords:
            all_coords.extend(ring)
    else:
        # Simple ring
        all_coords = polygon_coords
    
    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    
    return [min(lons), min(lats), max(lons), max(lats)]


def fetch_scooters(bbox, user_location, zoom, headers, delay=0.1):
    """Запрос самокатов для заданной области."""
    endpoint = "/4.0/eboks/scooters/v1/objects/discovery"
    url = f"{BASE_URL}{endpoint}"
    
    params = {
        "mobcf": "russia%25go_ru_by_geo_hosts_2%25default",
        "mobpr": "go_ru_by_geo_hosts_2_TAXI_V4_0"
    }
    
    data = {
        "actions": [],
        "bbox": bbox,
        "user_location": user_location,
        "zoom": zoom
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
        
        if response.status_code == 405:
            print("❌ Ошибка 405: JWT токен истёк!")
            sys.exit(1)
        elif response.status_code in [401, 403]:
            print(f"❌ Ошибка {response.status_code}: Токен недействителен!")
            sys.exit(1)
        
        response.raise_for_status()
        
        if delay > 0:
            time.sleep(delay)
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Ошибка запроса: {e}")
        return None


def extract_points_from_response(data):
    """
    Извлечение всех координат из ответа (любой формат).
    Возвращает list of [lon, lat].
    """
    points = []
    
    # objects формат (детальный)
    objects = data.get('objects', {})
    for obj_type in objects.get('objects_by_type', []):
        for obj in obj_type.get('objects', []):
            if isinstance(obj, dict) and 'geo' in obj:
                points.append(obj['geo'])
            elif isinstance(obj, list) and len(obj) >= 2:
                points.append(obj)
    
    # rowan формат (упрощенный)
    rowan = data.get('rowan', {})
    for obj_type in rowan.get('objects_by_type', []):
        for coords in obj_type.get('objects', []):
            if coords and len(coords) >= 2:
                points.append(coords)
    
    return points


def extract_detailed_objects(data):
    """
    Извлечение детальных объектов из objects формата.
    Возвращает dict: {scooters: [...], clusters: [...]}
    """
    result = {
        'scooters': [],
        'clusters': [],
        'cluster_empty': []
    }
    
    objects = data.get('objects', {})
    for obj_type in objects.get('objects_by_type', []):
        type_name = obj_type.get('type')
        objects_list = obj_type.get('objects', [])
        
        if type_name == 'scooter':
            result['scooters'].extend(objects_list)
        elif type_name == 'cluster':
            result['clusters'].extend(objects_list)
        elif type_name == 'cluster_empty':
            result['cluster_empty'].extend(objects_list)
    
    return result


def simple_cluster_points(points, grid_size_deg=0.02):
    """
    Простая кластеризация точек в сетку.
    Возвращает list of bboxes для "горячих" зон.
    """
    if not points:
        return []
    
    # Находим общий bbox
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    
    # Создаём сетку
    grid = defaultdict(list)
    
    for point in points:
        lon, lat = point
        grid_x = int((lon - min_lon) / grid_size_deg)
        grid_y = int((lat - min_lat) / grid_size_deg)
        grid[(grid_x, grid_y)].append(point)
    
    # Создаём bbox для непустых ячеек
    hot_zones = []
    for (grid_x, grid_y), cell_points in grid.items():
        if len(cell_points) > 0:  # Любое количество точек
            cell_min_lon = min_lon + grid_x * grid_size_deg
            cell_min_lat = min_lat + grid_y * grid_size_deg
            cell_max_lon = cell_min_lon + grid_size_deg
            cell_max_lat = cell_min_lat + grid_size_deg
            
            hot_zones.append({
                'bbox': [cell_min_lon, cell_min_lat, cell_max_lon, cell_max_lat],
                'points_count': len(cell_points)
            })
    
    return hot_zones


def shrink_bbox_around_point(point, size_deg=0.005):
    """Создание маленького bbox вокруг точки."""
    lon, lat = point
    return [
        lon - size_deg,
        lat - size_deg,
        lon + size_deg,
        lat + size_deg
    ]


def fetch_city_scooters(city_bbox, city_id, headers, min_cluster_size=50, delay=0.1):
    """
    Комбинированный подход для полного парсинга города.
    """
    print(f"\n🚀 Парсинг города: {city_id}")
    print("="*80)
    
    # Вычисляем центр bbox для user_location
    center_lon = (city_bbox[0] + city_bbox[2]) / 2
    center_lat = (city_bbox[1] + city_bbox[3]) / 2
    user_location = [center_lon, center_lat]
    
    # Этап 1: Обзорный запрос с низким zoom
    print(f"\n📡 Этап 1: Обзорный запрос (zoom 12)")
    print(f"   Bbox: {city_bbox}")
    
    overview_data = fetch_scooters(city_bbox, user_location, zoom=12, headers=headers, delay=delay)
    
    if not overview_data:
        print("❌ Не удалось получить обзорные данные")
        return {}
    
    # Извлекаем все точки
    all_points = extract_points_from_response(overview_data)
    print(f"   Найдено точек: {len(all_points)}")
    
    if len(all_points) == 0:
        print("   ℹ️  В городе нет самокатов")
        return {}
    
    # Этап 2: Кластеризация в горячие зоны
    print(f"\n🔥 Этап 2: Кластеризация точек (сетка 0.02°)")
    hot_zones = simple_cluster_points(all_points, grid_size_deg=0.02)
    print(f"   Горячих зон: {len(hot_zones)}")
    
    # Этап 3: Детальные запросы для горячих зон
    print(f"\n📥 Этап 3: Детальные запросы (zoom 17)")
    
    all_scooters = {}
    all_clusters_to_process = []
    
    for i, zone in enumerate(hot_zones, 1):
        zone_bbox = zone['bbox']
        zone_center = [
            (zone_bbox[0] + zone_bbox[2]) / 2,
            (zone_bbox[1] + zone_bbox[3]) / 2
        ]
        
        print(f"   [{i}/{len(hot_zones)}] Зона с {zone['points_count']} точками...", end=' ')
        
        detail_data = fetch_scooters(zone_bbox, zone_center, zoom=17, headers=headers, delay=delay)
        
        if not detail_data:
            print("⚠️  Ошибка")
            continue
        
        objects = extract_detailed_objects(detail_data)
        
        # Сохраняем самокаты
        for scooter in objects['scooters']:
            scooter_id = scooter.get('id')
            if scooter_id:
                all_scooters[scooter_id] = scooter
        
        # Собираем большие кластеры для дальнейшей обработки
        for cluster in objects['clusters']:
            count = cluster.get('payload', {}).get('objects_count', 0)
            if count >= min_cluster_size:
                all_clusters_to_process.append(cluster)
            else:
                # Маленькие кластеры сохраняем как есть
                cluster_id = cluster.get('id')
                if cluster_id:
                    all_scooters[cluster_id] = cluster
        
        print(f"✓ {len(objects['scooters'])} самокатов, {len(objects['clusters'])} кластеров")
    
    # Этап 4: Рекурсивное раскрытие больших кластеров
    if all_clusters_to_process:
        print(f"\n🔍 Этап 4: Раскрытие больших кластеров (zoom 19)")
        print(f"   Кластеров для обработки: {len(all_clusters_to_process)}")
        
        for i, cluster in enumerate(all_clusters_to_process, 1):
            count = cluster.get('payload', {}).get('objects_count', 0)
            geo = cluster.get('geo')
            
            print(f"   [{i}/{len(all_clusters_to_process)}] Кластер с {count} самокатами...", end=' ')
            
            if not geo:
                print("⚠️  Нет координат")
                continue
            
            # Уменьшаем bbox вокруг кластера
            small_bbox = shrink_bbox_around_point(geo, size_deg=0.005)
            
            detail_data = fetch_scooters(small_bbox, geo, zoom=19, headers=headers, delay=delay)
            
            if not detail_data:
                print("⚠️  Ошибка")
                # Сохраняем кластер как есть
                cluster_id = cluster.get('id')
                if cluster_id:
                    all_scooters[cluster_id] = cluster
                continue
            
            objects = extract_detailed_objects(detail_data)
            
            # Сохраняем раскрытые самокаты
            new_scooters = 0
            for scooter in objects['scooters']:
                scooter_id = scooter.get('id')
                if scooter_id and scooter_id not in all_scooters:
                    all_scooters[scooter_id] = scooter
                    new_scooters += 1
            
            # Если остались кластеры - сохраняем их
            for sub_cluster in objects['clusters']:
                cluster_id = sub_cluster.get('id')
                if cluster_id:
                    all_scooters[cluster_id] = sub_cluster
            
            print(f"✓ Раскрыто {new_scooters}/{count}")
    
    return all_scooters


def save_geojson(scooters_dict, output_path, city_id):
    """Сохранение результатов в GeoJSON."""
    features = []
    
    stats = {
        'scooters': 0,
        'clusters': 0,
        'cluster_scooters': 0
    }
    
    for obj_id, obj in scooters_dict.items():
        geo = obj.get('geo')
        if not geo:
            continue
        
        obj_type = obj_id.split('_')[0]
        
        properties = {
            "id": obj_id,
            "city_id": city_id
        }
        
        # Определяем тип и добавляем свойства
        if obj_type == 'scooter':
            properties["type"] = "scooter"
            properties["number"] = obj.get('payload', {}).get('number')
            stats['scooters'] += 1
        elif obj_type == 'cluster':
            properties["type"] = "cluster"
            count = obj.get('payload', {}).get('objects_count', 0)
            properties["objects_count"] = count
            properties["overlay_text"] = obj.get('overlay_text')
            stats['clusters'] += 1
            stats['cluster_scooters'] += count
        
        feature = {
            "type": "Feature",
            "id": obj_id,
            "geometry": {
                "type": "Point",
                "coordinates": geo
            },
            "properties": properties
        }
        
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "city_id": city_id,
            "generated_at": datetime.now().isoformat(),
            "total_objects": len(features),
            "scooters": stats['scooters'],
            "clusters": stats['clusters'],
            "cluster_scooters": stats['cluster_scooters'],
            "total_scooters": stats['scooters'] + stats['cluster_scooters'],
            "source": "Yandex Go API (Combined Approach)"
        }
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='Полный парсинг самокатов города')
    parser.add_argument('city_id', nargs='?', help='ID города из cities.geojson (например: polygon-184332)')
    parser.add_argument('--bbox', type=str, help='Custom bbox: min_lon,min_lat,max_lon,max_lat')
    parser.add_argument('--min-cluster', type=int, default=50,
                       help='Минимальный размер кластера для рекурсии (по умолчанию: 50)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Задержка между запросами в секундах (по умолчанию: 0.1)')
    
    args = parser.parse_args()
    
    # Загрузка конфигурации
    headers = load_config()
    
    # Определение bbox
    if args.bbox:
        parts = args.bbox.split(',')
        if len(parts) != 4:
            print("❌ Ошибка: bbox должен содержать 4 значения")
            sys.exit(1)
        city_bbox = [float(x) for x in parts]
        city_id = f"custom_{int(time.time())}"
    elif args.city_id:
        city_feature = load_city_polygon(args.city_id)
        city_bbox = get_polygon_bbox(city_feature['geometry']['coordinates'])
        city_id = args.city_id
    else:
        print("❌ Ошибка: укажите city_id или --bbox")
        parser.print_help()
        sys.exit(1)
    
    # Парсинг города
    start_time = time.time()
    
    scooters = fetch_city_scooters(
        city_bbox,
        city_id,
        headers,
        min_cluster_size=args.min_cluster,
        delay=args.delay
    )
    
    if not scooters:
        print("\n❌ Самокаты не найдены")
        sys.exit(0)
    
    # Сохранение результатов
    print(f"\n💾 Сохранение результатов...")
    
    base_dir = Path(__file__).parent
    output_dir = base_dir / 'output' / 'city_scooters'
    output_path = output_dir / f'{city_id}.geojson'
    
    stats = save_geojson(scooters, output_path, city_id)
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*80)
    print("✅ ГОТОВО!")
    print("="*80)
    print(f"📄 Файл: {output_path}")
    print(f"⏱️  Время: {elapsed:.1f} сек")
    print(f"\n📊 Статистика:")
    print(f"   Отдельных самокатов:     {stats['scooters']}")
    print(f"   Кластеров:               {stats['clusters']}")
    print(f"   Самокатов в кластерах:   {stats['cluster_scooters']}")
    print(f"   {'─'*40}")
    print(f"   ВСЕГО самокатов:         {stats['scooters'] + stats['cluster_scooters']}")


if __name__ == "__main__":
    main()
