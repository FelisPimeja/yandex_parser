#!/usr/bin/env python3
"""
Скрипт для загрузки списка городов из API Urent.
Сохраняет JSON в output/tmp/ и конвертирует в GeoJSON в output/.

Использование:
    python3 fetch_cities.py              # Загрузить и сконвертировать
    python3 fetch_cities.py --noexport   # Только загрузить JSON
"""

import json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
import requests
import urllib3

# Отключаем предупреждения о небезопасном SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_config():
    """Загрузка токена из config.json или переменной окружения."""
    token = os.environ.get('URENT_TOKEN')
    
    if not token:
        config_path = Path(__file__).parent / 'config.json'
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
                token = config.get('bearer_token')
    
    if not token:
        print("❌ Ошибка: токен не найден!")
        print("Создайте config.json или установите переменную URENT_TOKEN")
        sys.exit(1)
    
    return token


def fetch_cities(token):
    """Загрузка списка городов из API."""
    url = "https://backyard.urentbike.ru/gatewayclient/api/v3/zones/uses"
    
    # Параметры запроса (координаты Москвы)
    params = {
        'availableCityTypes': ['available', 'frozen'],
        'locationLat': 55.77545546986907,
        'locationLng': 37.63290022965542
    }
    
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    print("📥 Загружаю список городов...")
    
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    
    if response.status_code == 403:
        print("❌ Ошибка 403: Токен истёк или недействителен")
        print("Обновите токен в config.json")
        print(f"Debug: Response text: {response.text[:200]}")
        sys.exit(1)
    
    if response.status_code != 200:
        print(f"❌ Ошибка {response.status_code}")
        print(f"Response: {response.text[:500]}")
        sys.exit(1)
    
    response.raise_for_status()
    data = response.json()
    
    cities = data.get('data', [])
    print(f"✅ Загружено городов: {len(cities)}")
    
    # Подсчёт по статусам
    available = sum(1 for c in cities if c.get('cityAvailabilityStatus') == 'AVAILABLE')
    frozen = sum(1 for c in cities if c.get('cityAvailabilityStatus') == 'FROZEN')
    print(f"   - AVAILABLE: {available}")
    print(f"   - FROZEN: {frozen}")
    
    return data


def save_json(data, output_path):
    """Сохранение данных в JSON файл."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Сохранено: {output_path}")


def convert_to_geojson(json_data, output_path, include_frozen=False):
    """Конвертация данных городов в GeoJSON."""
    cities = json_data.get('data', [])
    
    features = []
    for city in cities:
        # Фильтр по статусу
        status = city.get('cityAvailabilityStatus', city.get('status'))
        if not include_frozen and status != 'AVAILABLE':
            continue
        
        # Конвертация координат в GeoJSON Polygon
        coordinates = city.get('coordinates', [])
        if not coordinates:
            continue
        
        # Преобразование [{lat, lng}, ...] -> [[lng, lat], ...]
        try:
            if isinstance(coordinates[0], dict):
                # Новый формат: {lat: ..., lng: ...}
                geojson_coords = [[[point['lng'], point['lat']] for point in coordinates]]
            else:
                # Старый формат: [lat, lng]
                geojson_coords = [[[point[1], point[0]] for point in coordinates]]
        except (KeyError, IndexError, TypeError) as e:
            print(f"⚠️  Пропускаю город {city.get('id')}: ошибка в координатах ({e})")
            continue
        
        feature = {
            "type": "Feature",
            "id": city.get('id'),
            "properties": {
                "id": city.get('id'),
                "cityId": city.get('cityId'),
                "status": status,
                "modalities": city.get('modalities', []),
                "centerLat": city.get('center', {}).get('lat'),
                "centerLng": city.get('center', {}).get('lng')
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": geojson_coords
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"📦 Создан GeoJSON: {output_path} ({len(features)} городов)")


def main():
    parser = argparse.ArgumentParser(
        description='Загрузка списка городов Urent'
    )
    parser.add_argument(
        '--noexport',
        action='store_true',
        help='Не конвертировать в GeoJSON, только сохранить JSON'
    )
    args = parser.parse_args()
    
    # Загрузка токена
    token = load_config()
    
    # Загрузка данных
    data = fetch_cities(token)
    
    # Пути для сохранения
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    output_dir = base_dir / 'output'
    
    json_path = tmp_dir / 'cities.json'
    
    # Сохранение JSON
    save_json(data, json_path)
    
    # Конвертация в GeoJSON (если не --noexport)
    if not args.noexport:
        print("\n📍 Конвертирую в GeoJSON...")
        
        # Все города (AVAILABLE + FROZEN)
        geojson_path = output_dir / 'cities.geojson'
        convert_to_geojson(data, geojson_path, include_frozen=True)
    
    print("\n✅ Готово!")


if __name__ == '__main__':
    main()
