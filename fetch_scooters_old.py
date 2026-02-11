#!/usr/bin/env python3
"""
Скрипт для загрузки самокатов Yandex Go в заданной области.
Сохраняет JSON в output/tmp/ и конвертирует в GeoJSON в output/.

Использование:
    python3 fetch_scooters.py                                # Москва (по умолчанию)
    python3 fetch_scooters.py --bbox 37.4,55.6,37.9,55.9     # Своя область
    python3 fetch_scooters.py --location 37.6,55.75          # С указанием местоположения
    python3 fetch_scooters.py --zoom 14.5                    # С другим zoom
"""

import json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
import requests

# Базовый URL API Yandex
BASE_URL = "https://tc.mobile.yandex.net"


def load_config():
    """Загрузка заголовков из config.json."""
    config_path = Path(__file__).parent / 'config.json'
    
    if not config_path.exists():
        print("❌ Ошибка: файл config.json не найден!")
        print("Создайте его на основе config.json.example")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Попробуем оба варианта для обратной совместимости
    headers = config.get('headers') or config.get('yandex_headers')
    
    if not headers:
        print("❌ Ошибка: заголовки не найдены в config.json!")
        print("Проверьте, что config.json содержит поле 'headers' или 'yandex_headers'")
        sys.exit(1)
    
    return headers


def fetch_scooters(bbox, user_location, zoom=12, headers=None):
    """
    Загрузка самокатов в заданной области.
    
    Args:
        bbox: list [min_lon, min_lat, max_lon, max_lat]
        user_location: list [lon, lat] - местоположение пользователя
        zoom: float, уровень зума карты
        headers: dict, заголовки запроса
    
    Returns:
        dict с данными самокатов
    """
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
    
    print(f"📥 Загружаю самокаты для области {bbox}...")
    print(f"   Местоположение: {user_location}")
    print(f"   URL: {url}")
    
    try:
        response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
        
        if response.status_code == 401:
            print("❌ Ошибка 401: Не авторизован. Проверьте токены в config.json")
            sys.exit(1)
        elif response.status_code == 403:
            print("❌ Ошибка 403: Доступ запрещен. Токен истёк или недействителен")
            sys.exit(1)
        elif response.status_code == 405:
            print("❌ Ошибка 405: JWT токен истёк. Обновите X-Yandex-Jws в config.json")
            sys.exit(1)
        
        response.raise_for_status()
        result = response.json()
        
        # Подсчёт самокатов и кластеров
        total_scooters = 0
        total_clusters = 0
        cluster_scooters = 0
        
        # objects (детальный формат при высоком zoom)
        objects = result.get('objects', {})
        objects_by_type = objects.get('objects_by_type', [])
        
        for obj_type in objects_by_type:
            type_name = obj_type.get('type', '')
            objects_list = obj_type.get('objects', [])
            
            if type_name == 'scooter':
                total_scooters += len(objects_list)
            elif type_name == 'cluster':
                total_clusters += len(objects_list)
                # Подсчитываем самокаты в кластерах
                for cluster in objects_list:
                    count = cluster.get('payload', {}).get('objects_count', 0)
                    cluster_scooters += count
        
        # rowan (упрощенный формат при низком zoom)
        rowan = result.get('rowan', {})
        rowan_objects = rowan.get('objects_by_type', [])
        
        for obj_type in rowan_objects:
            type_name = obj_type.get('type', '')
            objects_list = obj_type.get('objects', [])
            
            if type_name == 'rowan_scooter':
                total_scooters += len(objects_list)
            elif type_name == 'rowan_cluster':
                total_clusters += len(objects_list)
        
        print(f"✅ Загружено самокатов: {total_scooters}")
        if total_clusters > 0:
            print(f"   Кластеров: {total_clusters}")
            if cluster_scooters > 0:
                print(f"   Самокатов в кластерах: {cluster_scooters}")
                print(f"   ВСЕГО самокатов: {total_scooters + cluster_scooters}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
        sys.exit(1)


def save_json(data, output_path):
    """Сохранение данных в JSON файл."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Сохранено: {output_path}")


def convert_to_geojson(scooters_data, output_path):
    """
    Конвертация данных самокатов в GeoJSON.
    
    Поддерживает два формата ответа API:
    
    1. Низкий zoom (10-13) - rowan формат:
       rowan.objects_by_type[].objects = [[lon, lat], ...]
    
    2. Высокий zoom (14+) - objects формат:
       objects.objects_by_type[].objects = [{id, geo, payload}, ...]
    """
    features = []
    scooter_id_counter = 1
    cluster_id_counter = 1
    
    # Обрабатываем objects (детальный формат)
    objects = scooters_data.get('objects', {})
    objects_by_type = objects.get('objects_by_type', [])
    
    for obj_type in objects_by_type:
        type_name = obj_type.get('type', 'unknown')
        objects_list = obj_type.get('objects', [])
        
        for obj in objects_list:
            # Детальный формат с полями id, geo, payload
            if isinstance(obj, dict):
                obj_id = obj.get('id', f'{type_name}_{scooter_id_counter}')
                geo = obj.get('geo')
                payload = obj.get('payload', {})
                
                if not geo or len(geo) < 2:
                    continue
                
                properties = {
                    "id": obj_id,
                    "type": type_name,
                    "source": "objects"
                }
                
                # Добавляем поля из payload
                if type_name == 'scooter':
                    properties["number"] = payload.get('number')
                elif type_name == 'cluster':
                    properties["objects_count"] = payload.get('objects_count')
                    properties["cluster_id"] = payload.get('cluster_id')
                elif type_name == 'cluster_empty':
                    properties["is_empty"] = True
                
                # Добавляем overlay_text если есть
                if 'overlay_text' in obj:
                    properties["overlay_text"] = obj['overlay_text']
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": geo  # [lon, lat]
                    },
                    "properties": properties
                }
                features.append(feature)
                
                if type_name == 'scooter':
                    scooter_id_counter += 1
                elif type_name in ['cluster', 'cluster_empty']:
                    cluster_id_counter += 1
            
            # Упрощенный формат - просто координаты
            elif isinstance(obj, list) and len(obj) >= 2:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": obj  # [lon, lat]
                    },
                    "properties": {
                        "id": f"obj_{scooter_id_counter}",
                        "type": type_name,
                        "source": "objects"
                    }
                }
                features.append(feature)
                scooter_id_counter += 1
    
    # Обрабатываем rowan (упрощенный формат)
    rowan = scooters_data.get('rowan', {})
    rowan_objects = rowan.get('objects_by_type', [])
    
    for obj_type in rowan_objects:
        type_name = obj_type.get('type', 'unknown')
        objects_list = obj_type.get('objects', [])
        
        for coords in objects_list:
            if not coords or len(coords) < 2:
                continue
            
            # Определяем ID в зависимости от типа
            if type_name == 'rowan_scooter':
                obj_id = f"scooter_{scooter_id_counter}"
                scooter_id_counter += 1
            elif type_name == 'rowan_cluster':
                obj_id = f"cluster_{cluster_id_counter}"
                cluster_id_counter += 1
            else:
                obj_id = f"unknown_{scooter_id_counter}"
                scooter_id_counter += 1
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": coords  # [lon, lat]
                },
                "properties": {
                    "id": obj_id,
                    "type": type_name,
                    "source": "rowan"
                }
            }
            features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "total_objects": len(features),
            "source": "Yandex Go API"
        }
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"📄 GeoJSON сохранён: {output_path}")
    print(f"   Всего объектов: {len(features)}")
    
    # Статистика по типам
    types_count = {}
    for feature in features:
        type_name = feature['properties']['type']
        types_count[type_name] = types_count.get(type_name, 0) + 1
    
    if types_count:
        print(f"   По типам:")
        for type_name, count in sorted(types_count.items()):
            print(f"     - {type_name}: {count}")


def parse_bbox(bbox_str):
    """Парсинг строки bbox в список чисел."""
    try:
        parts = bbox_str.split(',')
        if len(parts) != 4:
            raise ValueError("bbox должен содержать 4 значения")
        return [float(x) for x in parts]
    except (ValueError, AttributeError) as e:
        print(f"❌ Ошибка парсинга bbox: {e}")
        print("Формат: min_lon,min_lat,max_lon,max_lat")
        sys.exit(1)


def parse_location(location_str):
    """Парсинг строки location в список чисел."""
    try:
        parts = location_str.split(',')
        if len(parts) != 2:
            raise ValueError("location должен содержать 2 значения")
        return [float(x) for x in parts]
    except (ValueError, AttributeError) as e:
        print(f"❌ Ошибка парсинга location: {e}")
        print("Формат: lon,lat")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Загрузка самокатов Yandex Go')
    parser.add_argument('--bbox', type=str, 
                       help='Bounding box (min_lon,min_lat,max_lon,max_lat). По умолчанию: Москва',
                       default='37.4,55.6,37.9,55.9')
    parser.add_argument('--location', type=str,
                       help='Местоположение пользователя (lon,lat). По умолчанию: центр bbox',
                       default=None)
    parser.add_argument('--zoom', type=float, default=12.0,
                       help='Уровень зума (по умолчанию: 12.0)')
    parser.add_argument('--noexport', action='store_true',
                       help='Не конвертировать в GeoJSON')
    
    args = parser.parse_args()
    
    print("🚀 Загрузка самокатов Yandex Go")
    print("="*80)
    
    # Загрузка конфигурации
    headers = load_config()
    
    # Парсинг bbox
    bbox = parse_bbox(args.bbox)
    print(f"📍 Область: {bbox}")
    
    # Определение user_location
    if args.location:
        user_location = parse_location(args.location)
    else:
        # Вычисляем центр bbox
        center_lon = (bbox[0] + bbox[2]) / 2
        center_lat = (bbox[1] + bbox[3]) / 2
        user_location = [center_lon, center_lat]
    
    print(f"📍 Местоположение: {user_location}")
    print(f"🔍 Zoom: {args.zoom}")
    print()
    
    # Загрузка данных
    scooters_data = fetch_scooters(bbox, user_location, args.zoom, headers)
    
    # Создание директорий
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    output_dir = base_dir / 'output'
    
    # Сохранение JSON
    json_path = tmp_dir / 'scooters.json'
    save_json(scooters_data, json_path)
    
    # Конвертация в GeoJSON
    if not args.noexport:
        geojson_path = output_dir / 'scooters.geojson'
        convert_to_geojson(scooters_data, geojson_path)
    
    print()
    print("="*80)
    print("✅ Готово!")


if __name__ == "__main__":
    main()
