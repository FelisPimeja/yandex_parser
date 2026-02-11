#!/usr/bin/env python3
"""
Скрипт для загрузки самокатов Yandex Go в заданной области.
Сохраняет JSON в output/tmp/ и конвертирует в GeoJSON в output/.

Использование:
    python3 fetch_scooters.py                    # Москва (по умолчанию)
    python3 fetch_scooters.py --bbox 37.4,55.6,37.9,55.9  # Своя область
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
    
    headers = config.get('yandex_headers')
    
    if not headers:
        print("❌ Ошибка: заголовки не найдены в config.json!")
        sys.exit(1)
    
    return headers


def fetch_scooters(bbox, zoom=12, headers=None):
    """
    Загрузка самокатов в заданной области.
    
    Args:
        bbox: list [min_lon, min_lat, max_lon, max_lat]
        zoom: int, уровень зума карты
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
        "bbox": bbox,
        "zoom": zoom
    }
    
    print(f"📥 Загружаю самокаты для области {bbox}...")
    print(f"   URL: {url}")
    
    try:
        response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
        
        if response.status_code == 401:
            print("❌ Ошибка 401: Не авторизован. Проверьте токены в config.json")
            sys.exit(1)
        elif response.status_code == 403:
            print("❌ Ошибка 403: Доступ запрещен. Токен истёк или недействителен")
            sys.exit(1)
        
        response.raise_for_status()
        result = response.json()
        
        # Подсчёт самокатов
        objects = result.get('objects', {})
        objects_by_type = objects.get('objects_by_type', [])
        
        total_scooters = 0
        for obj_type in objects_by_type:
            items = obj_type.get('items', [])
            total_scooters += len(items)
        
        rowan = result.get('rowan', {})
        rowan_objects = rowan.get('objects_by_type', [])
        
        for obj_type in rowan_objects:
            items = obj_type.get('items', [])
            total_scooters += len(items)
        
        print(f"✅ Загружено самокатов: {total_scooters}")
        
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
    
    Структура ответа:
    {
      "objects": {
        "objects_by_type": [
          {
            "type": "...",
            "items": [
              {
                "id": "...",
                "position": [lon, lat],
                "charge": 85,
                ...
              }
            ]
          }
        ]
      },
      "rowan": { ... }
    }
    """
    features = []
    
    # Обрабатываем objects
    objects = scooters_data.get('objects', {})
    objects_by_type = objects.get('objects_by_type', [])
    
    for obj_type in objects_by_type:
        type_name = obj_type.get('type', 'unknown')
        items = obj_type.get('items', [])
        
        for item in items:
            position = item.get('position')
            if not position or len(position) < 2:
                continue
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": position  # [lon, lat]
                },
                "properties": {
                    "id": item.get('id'),
                    "type": type_name,
                    "charge": item.get('charge'),
                    "number": item.get('number'),
                    "source": "objects"
                }
            }
            
            # Добавляем все остальные поля как properties
            for key, value in item.items():
                if key not in ['id', 'position', 'charge', 'number']:
                    feature['properties'][key] = value
            
            features.append(feature)
    
    # Обрабатываем rowan (если есть)
    rowan = scooters_data.get('rowan', {})
    rowan_objects = rowan.get('objects_by_type', [])
    
    for obj_type in rowan_objects:
        type_name = obj_type.get('type', 'unknown')
        items = obj_type.get('items', [])
        
        for item in items:
            position = item.get('position')
            if not position or len(position) < 2:
                continue
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": position
                },
                "properties": {
                    "id": item.get('id'),
                    "type": type_name,
                    "charge": item.get('charge'),
                    "number": item.get('number'),
                    "source": "rowan"
                }
            }
            
            for key, value in item.items():
                if key not in ['id', 'position', 'charge', 'number']:
                    feature['properties'][key] = value
            
            features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "total_scooters": len(features),
            "source": "Yandex Go API"
        }
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"📄 GeoJSON сохранён: {output_path}")
    print(f"   Всего объектов: {len(features)}")


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


def main():
    parser = argparse.ArgumentParser(description='Загрузка самокатов Yandex Go')
    parser.add_argument('--bbox', type=str, 
                       help='Bounding box (min_lon,min_lat,max_lon,max_lat). По умолчанию: Москва',
                       default='37.4,55.6,37.9,55.9')
    parser.add_argument('--zoom', type=int, default=12,
                       help='Уровень зума (по умолчанию: 12)')
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
    print(f"🔍 Zoom: {args.zoom}")
    print()
    
    # Загрузка данных
    scooters_data = fetch_scooters(bbox, args.zoom, headers)
    
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
