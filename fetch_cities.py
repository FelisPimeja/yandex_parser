#!/usr/bin/env python3
"""
Скрипт для загрузки списка городов из API Urent.
Объединяет данные из v1 (детальная информация) и v3 (полный список).
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


def fetch_cities_v1(token):
    """
    Загрузка списка городов из API v1.
    Возвращает детальную информацию: названия, скорости, bounding box.
    Обычно возвращает ~36 активных зон.
    """
    url = "https://backyard.urentbike.ru/gatewayclient/api/v1/zones/uses"
    
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    print("📥 Загружаю детальную информацию о городах (v1)...")
    
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    
    if response.status_code == 403:
        print("❌ Ошибка 403: Токен истёк или недействителен")
        print("Обновите токен в config.json")
        sys.exit(1)
    
    response.raise_for_status()
    data = response.json()
    
    cities = data.get('entries', [])
    print(f"✅ v1: Загружено {len(cities)} зон с детальной информацией")
    
    return data


def fetch_cities_v3(token):
    """
    Загрузка списка городов из API v3.
    Возвращает полный список boundary зон включая FROZEN.
    Обычно возвращает ~355 зон (с дубликатами и подзонами).
    """
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
    
    print("📥 Загружаю полный список городов (v3)...")
    
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    
    if response.status_code == 403:
        print("❌ Ошибка 403: Токен истёк или недействителен")
        print("Обновите токен в config.json")
        sys.exit(1)
    
    response.raise_for_status()
    data = response.json()
    
    cities = data.get('data', [])
    print(f"✅ v3: Загружено {len(cities)} boundary зон")
    
    # Подсчёт по статусам
    available = sum(1 for c in cities if c.get('cityAvailabilityStatus') == 'AVAILABLE')
    frozen = sum(1 for c in cities if c.get('cityAvailabilityStatus') == 'FROZEN')
    print(f"   - AVAILABLE: {available}")
    print(f"   - FROZEN: {frozen}")
    
    return data


def merge_city_data(v1_data, v3_data):
    """
    Объединение данных из v1 и v3.
    
    v1 содержит детальную информацию (названия, скорости, bounding box).
    v3 содержит полный список boundary зон.
    
    Логика:
    1. Берём все города из v3 как основу
    2. Добавляем детальную информацию из v1 по совпадению areaId (v1) = cityId (v3)
    """
    v1_cities = v1_data.get('entries', [])
    v3_cities = v3_data.get('data', [])
    
    print("\n🔗 Объединяю данные из v1 и v3...")
    
    # Создаём индекс v1 по areaId для быстрого поиска
    v1_by_area_id = {}
    for city in v1_cities:
        area_id = city.get('areaId')
        if area_id:
            # Если несколько зон для одного areaId, сохраняем первую
            if area_id not in v1_by_area_id:
                v1_by_area_id[area_id] = city
    
    print(f"   📊 v1: {len(v1_by_area_id)} уникальных areaId")
    
    # Обогащаем данные v3 информацией из v1
    enriched_cities = []
    matched = 0
    
    for v3_city in v3_cities:
        city_id = v3_city.get('cityId')
        
        # Объединяем данные
        merged_city = v3_city.copy()
        
        # Если есть детальная информация в v1, добавляем её
        if city_id and city_id in v1_by_area_id:
            v1_city = v1_by_area_id[city_id]
            matched += 1
            
            # Добавляем поля из v1
            merged_city['name'] = v1_city.get('name')
            merged_city['description'] = v1_city.get('description')
            merged_city['areaId'] = v1_city.get('areaId')
            merged_city['disabled'] = v1_city.get('disabled')
            merged_city['restricted'] = v1_city.get('restricted')
            merged_city['freefloat'] = v1_city.get('freefloat')
            merged_city['northWest'] = v1_city.get('northWest')
            merged_city['southEast'] = v1_city.get('southEast')
            merged_city['normalSpeedValue'] = v1_city.get('normalSpeedValue')
            merged_city['speedLimitValue'] = v1_city.get('speedLimitValue')
            merged_city['speedLimitMode'] = v1_city.get('speedLimitMode')
            merged_city['alarmSpeedValue'] = v1_city.get('alarmSpeedValue')
            merged_city['alarmMode'] = v1_city.get('alarmMode')
            merged_city['transportCapacities'] = v1_city.get('transportCapacities')
        
        enriched_cities.append(merged_city)
    
    print(f"   ✅ Обогащено {matched} из {len(v3_cities)} городов данными из v1")
    
    # Формируем итоговый результат
    result = {
        'data': enriched_cities,
        'meta': {
            'v1_zones': len(v1_cities),
            'v3_zones': len(v3_cities),
            'enriched': matched,
            'timestamp': datetime.now().isoformat()
        },
        'errors': [],
        'succeeded': True
    }
    
    return result


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
        
        # Базовые свойства (всегда есть в v3)
        properties = {
            "id": city.get('id'),
            "cityId": city.get('cityId'),
            "status": status,
            "modalities": city.get('modalities', []),
            "centerLat": city.get('center', {}).get('lat'),
            "centerLng": city.get('center', {}).get('lng')
        }
        
        # Дополнительные свойства из v1 (если есть)
        if city.get('name'):
            properties['name'] = city.get('name')
        if city.get('description'):
            properties['description'] = city.get('description')
        if city.get('areaId'):
            properties['areaId'] = city.get('areaId')
        
        # Скоростные режимы
        if city.get('normalSpeedValue') is not None:
            properties['normalSpeed'] = city.get('normalSpeedValue')
        if city.get('speedLimitValue') is not None:
            properties['speedLimit'] = city.get('speedLimitValue')
        if city.get('alarmSpeedValue') is not None:
            properties['alarmSpeed'] = city.get('alarmSpeedValue')
        if city.get('speedLimitMode') is not None:
            properties['speedLimitMode'] = city.get('speedLimitMode')
        if city.get('alarmMode') is not None:
            properties['alarmMode'] = city.get('alarmMode')
        
        # Статусы и режимы
        if city.get('disabled') is not None:
            properties['disabled'] = city.get('disabled')
        if city.get('restricted') is not None:
            properties['restricted'] = city.get('restricted')
        if city.get('freefloat') is not None:
            properties['freefloat'] = city.get('freefloat')
        
        # Bounding box
        if city.get('northWest'):
            nw = city.get('northWest', {})
            properties['boundingBoxNorthWestLat'] = nw.get('lat')
            properties['boundingBoxNorthWestLng'] = nw.get('lng')
        if city.get('southEast'):
            se = city.get('southEast', {})
            properties['boundingBoxSouthEastLat'] = se.get('lat')
            properties['boundingBoxSouthEastLng'] = se.get('lng')
        
        # Информация о транспорте (если есть)
        if city.get('transportCapacities'):
            capacities = city.get('transportCapacities', [])
            for cap in capacities:
                transport_type = cap.get('transportType', '').lower()
                count = cap.get('count', 0)
                if transport_type:
                    properties[f'{transport_type}Count'] = count
        
        feature = {
            "type": "Feature",
            "id": city.get('id'),
            "properties": properties,
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
        description='Загрузка списка городов Urent из v1 (детали) и v3 (полный список)'
    )
    parser.add_argument(
        '--noexport',
        action='store_true',
        help='Не конвертировать в GeoJSON, только сохранить JSON'
    )
    args = parser.parse_args()
    
    # Загрузка токена
    token = load_config()
    
    print("🚀 Начинаю загрузку городов...\n")
    
    # Загрузка данных из обоих API
    v1_data = fetch_cities_v1(token)
    v3_data = fetch_cities_v3(token)
    
    # Объединение данных
    merged_data = merge_city_data(v1_data, v3_data)
    
    # Пути для сохранения
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    output_dir = base_dir / 'output'
    
    json_path = tmp_dir / 'cities.json'
    
    # Сохранение JSON
    print(f"\n💾 Сохраняю объединённые данные...")
    save_json(merged_data, json_path)
    
    # Конвертация в GeoJSON (если не --noexport)
    if not args.noexport:
        print("\n📍 Конвертирую в GeoJSON...")
        
        # Все города (AVAILABLE + FROZEN)
        geojson_path = output_dir / 'cities.geojson'
        convert_to_geojson(merged_data, geojson_path, include_frozen=True)
    
    print("\n✅ Готово!")
    print(f"   📊 Всего зон: {len(merged_data['data'])}")
    print(f"   🔗 Обогащено данными v1: {merged_data['meta']['enriched']}")


if __name__ == '__main__':
    main()
