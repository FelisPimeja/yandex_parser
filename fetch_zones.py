#!/usr/bin/env python3
"""
Скрипт для загрузки всех зон (аренды и ограничений) для всех городов.
Использует fetch_cities.py для получения списка городов.

Сохраняет JSON в output/tmp/ и конвертирует в GeoJSON в output/.

Использование:
    python3 fetch_zones.py
"""

import json
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import requests
import urllib3

# Отключаем предупреждения о небезопасном SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def convert_coordinates_to_geojson(coordinates):
    """
    Универсальная функция для конвертации координат в GeoJSON формат.
    Поддерживает оба формата: [{lat, lng}, ...] и [[lat, lng], ...]
    Возвращает: [[[lng, lat], ...]] для GeoJSON Polygon
    """
    if not coordinates:
        return None
    
    try:
        if isinstance(coordinates[0], dict):
            # Новый формат: {lat: ..., lng: ...}
            return [[[point['lng'], point['lat']] for point in coordinates]]
        else:
            # Старый формат: [lat, lng]
            return [[[point[1], point[0]] for point in coordinates]]
    except (KeyError, IndexError, TypeError) as e:
        print(f"⚠️  Ошибка конвертации координат: {e}")
        return None


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


def get_cities():
    """Получение списка городов через fetch_cities.py."""
    base_dir = Path(__file__).parent
    cities_json_path = base_dir / 'output' / 'tmp' / 'cities.json'
    
    # Проверяем наличие файла
    if not cities_json_path.exists():
        print("📥 Файл cities.json не найден, загружаю...")
        # Запускаем fetch_cities.py с флагом --noexport
        result = subprocess.run(
            [sys.executable, 'fetch_cities.py', '--noexport'],
            cwd=base_dir,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"❌ Ошибка при загрузке городов:\n{result.stderr}")
            sys.exit(1)
        print(result.stdout)
    
    # Загружаем список городов
    with open(cities_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cities = data.get('data', [])
    available_cities = [c for c in cities if c.get('cityAvailabilityStatus') == 'AVAILABLE']
    
    print(f"📋 Найдено городов: {len(available_cities)} (AVAILABLE)")
    return available_cities


def fetch_rent_zones(city_id, token):
    """Загрузка зон аренды для города."""
    url = f"https://backyard.urentbike.ru/gatewayclient/api/v3/zones/rent?cityId={city_id}"
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    
    if response.status_code == 403:
        print("❌ Ошибка 403: Токен истёк или недействителен")
        sys.exit(1)
    
    response.raise_for_status()
    return response.json()


def fetch_restriction_zones(rent_zone_id, token):
    """Загрузка зон ограничений для rent zone."""
    url = f"https://backyard.urentbike.ru/gatewayclient/api/v5/zones/general?rentZoneId={rent_zone_id}"
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    return response.json()


def save_json(data, output_path):
    """Сохранение данных в JSON файл."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def convert_zones_to_geojson(all_zones_data, output_path):
    """Конвертация зон аренды и ограничений в GeoJSON."""
    features = []
    
    for city_data in all_zones_data:
        city_name = city_data['city_name']
        
        # Обработка rent zones
        for rent_zone in city_data.get('rent_zones', []):
            coordinates = rent_zone.get('coordinates', [])
            geojson_coords = convert_coordinates_to_geojson(coordinates)
            if not geojson_coords:
                continue
            
            feature = {
                "type": "Feature",
                "id": rent_zone.get('id'),
                "properties": {
                    "id": rent_zone.get('id'),
                    "name": rent_zone.get('name'),
                    "city": city_name,
                    "type": "rentZone",
                    "status": rent_zone.get('status')
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": geojson_coords
                }
            }
            features.append(feature)
        
        # Обработка restriction zones
        for restriction_data in city_data.get('restrictions', []):
            rent_zone_id = restriction_data['rent_zone_id']
            general_zones = restriction_data.get('general_zones', {}).get('data', {})
            
            # Low speed zones
            for zone in general_zones.get('lowSpeedZones', []):
                coordinates = zone.get('coordinates', [])
                geojson_coords = convert_coordinates_to_geojson(coordinates)
                if not geojson_coords:
                    continue
                
                feature = {
                    "type": "Feature",
                    "id": zone.get('id'),
                    "properties": {
                        "id": zone.get('id'),
                        "city": city_name,
                        "type": "lowSpeedZone",
                        "rentZoneId": rent_zone_id,
                        "speedLimitValue": zone.get('speedLimitValue')
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": geojson_coords
                    }
                }
                features.append(feature)
            
            # Restricted zones (запрет парковки)
            for zone in general_zones.get('restrictedZones', []):
                coordinates = zone.get('coordinates', [])
                geojson_coords = convert_coordinates_to_geojson(coordinates)
                if not geojson_coords:
                    continue
                
                feature = {
                    "type": "Feature",
                    "id": zone.get('id'),
                    "properties": {
                        "id": zone.get('id'),
                        "city": city_name,
                        "type": "restrictedZone",
                        "rentZoneId": rent_zone_id
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": geojson_coords
                    }
                }
                features.append(feature)
            
            # Not allowed zones (запрет поездок)
            for zone in general_zones.get('notAllowedZones', []):
                coordinates = zone.get('coordinates', [])
                geojson_coords = convert_coordinates_to_geojson(coordinates)
                if not geojson_coords:
                    continue
                
                feature = {
                    "type": "Feature",
                    "id": zone.get('id'),
                    "properties": {
                        "id": zone.get('id'),
                        "city": city_name,
                        "type": "notAllowedZone",
                        "rentZoneId": rent_zone_id
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
    
    print(f"📦 Создан GeoJSON: {output_path} ({len(features)} зон)")


def main():
    print("🚀 Начинаю загрузку зон аренды и ограничений...\n")
    
    # Загрузка токена
    token = load_config()
    
    # Получение списка городов
    cities = get_cities()
    
    # Структура для хранения всех данных
    all_zones_data = []
    
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    
    # Обработка каждого города
    for i, city in enumerate(cities, 1):
        city_id = city['cityId']  # ID города для запроса rent zones
        city_boundary_id = city['id']  # ID границы города
        city_name = city.get('name', city_id)  # Используем cityId если name нет
        
        print(f"\n[{i}/{len(cities)}] 🏙️  {city_name}")
        
        city_data = {
            'city_id': city_id,
            'city_boundary_id': city_boundary_id,
            'city_name': city_name,
            'rent_zones': [],
            'restrictions': []
        }
        
        # Загрузка rent zones
        try:
            print(f"  📥 Загружаю rent zones...")
            rent_zones_data = fetch_rent_zones(city_id, token)
            rent_zones = rent_zones_data.get('data', [])
            city_data['rent_zones'] = rent_zones
            print(f"  ✅ Rent zones: {len(rent_zones)}")
            
            # Сохранение rent zones в tmp
            rent_zones_path = tmp_dir / f'rent_zones_{city_id}.json'
            save_json(rent_zones_data, rent_zones_path)
            
        except Exception as e:
            print(f"  ⚠️  Ошибка при загрузке rent zones: {e}")
            continue
        
        # Загрузка restriction zones для каждой rent zone
        if rent_zones:
            print(f"  📥 Загружаю restriction zones...")
            for rent_zone in rent_zones:
                rent_zone_id = rent_zone['id']
                
                try:
                    restriction_data = fetch_restriction_zones(rent_zone_id, token)
                    city_data['restrictions'].append({
                        'rent_zone_id': rent_zone_id,
                        'general_zones': restriction_data
                    })
                    
                    # Сохранение restriction zones в tmp
                    restriction_path = tmp_dir / f'restrictions_{rent_zone_id}.json'
                    save_json(restriction_data, restriction_path)
                    
                except Exception as e:
                    print(f"  ⚠️  Ошибка для rent zone {rent_zone_id}: {e}")
                    continue
            
            # Подсчёт общего количества restriction zones
            total_restrictions = 0
            for restriction in city_data['restrictions']:
                general = restriction.get('general_zones', {}).get('data', {})
                total_restrictions += len(general.get('lowSpeedZones', []))
                total_restrictions += len(general.get('restrictedZones', []))
                total_restrictions += len(general.get('notAllowedZones', []))
            
            print(f"  ✅ Restriction zones: {total_restrictions}")
        
        all_zones_data.append(city_data)
    
    # Сохранение объединённых данных
    print("\n💾 Сохраняю данные...")
    all_data_path = tmp_dir / 'all_zones.json'
    save_json(all_zones_data, all_data_path)
    print(f"💾 Сохранено: {all_data_path}")
    
    # Конвертация в GeoJSON
    print("\n📍 Конвертирую в GeoJSON...")
    output_dir = base_dir / 'output'
    geojson_path = output_dir / 'zones.geojson'
    convert_zones_to_geojson(all_zones_data, geojson_path)
    
    print("\n✅ Готово!")
    print(f"   Обработано городов: {len(all_zones_data)}")


if __name__ == '__main__':
    main()
