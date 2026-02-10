#!/usr/bin/env python3
"""
Скрипт для загрузки парковок и доступного транспорта для всех городов.
Использует fetch_cities.py для получения списка городов.

Для каждой зоны аренды делает запрос с центром зоны и радиусом 10км.
Сохраняет JSON в output/tmp/ и конвертирует в GeoJSON в output/.

Использование:
    python3 fetch_parkings.py
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


def calculate_center(coordinates):
    """Вычисление центра полигона. Поддерживает оба формата координат."""
    if not coordinates:
        return None
    
    try:
        if isinstance(coordinates[0], dict):
            # Новый формат: {lat: ..., lng: ...}
            lats = [point['lat'] for point in coordinates]
            lngs = [point['lng'] for point in coordinates]
        else:
            # Старый формат: [lat, lng]
            lats = [point[0] for point in coordinates]
            lngs = [point[1] for point in coordinates]
        
        center_lat = sum(lats) / len(lats)
        center_lng = sum(lngs) / len(lngs)
        
        return {
            "latitude": center_lat,
            "longitude": center_lng
        }
    except (KeyError, IndexError, TypeError) as e:
        print(f"⚠️  Ошибка вычисления центра: {e}")
        return None


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


def fetch_transports(center, token, radius_meters=10000):
    """
    Загрузка доступного транспорта в радиусе от центра.
    
    API endpoint: GET /gatewayclient/api/v6/transports
    Query params:
        - startLatitude, startLongitude - координаты точки отправления
        - latitude, longitude - координаты центра поиска
        - radius - радиус поиска в метрах
    """
    url = "https://backyard.urentbike.ru/gatewayclient/api/v6/transports"
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    params = {
        "startLatitude": center["latitude"],
        "startLongitude": center["longitude"],
        "latitude": center["latitude"],
        "longitude": center["longitude"],
        "radius": radius_meters
    }
    
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    response.raise_for_status()
    return response.json()


def save_json(data, output_path):
    """Сохранение данных в JSON файл."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def convert_parkings_to_geojson(all_parkings_data, output_path):
    """Конвертация парковок в GeoJSON."""
    features = []
    seen_parking_ids = set()
    
    for city_data in all_parkings_data:
        city_name = city_data['city_name']
        
        for zone_data in city_data.get('zones', []):
            transports_data = zone_data.get('transports', {}).get('data', {})
            parkings = transports_data.get('parkingList', [])
            
            for parking in parkings:
                parking_id = parking.get('id')
                
                # Избегаем дубликатов парковок
                if parking_id in seen_parking_ids:
                    continue
                seen_parking_ids.add(parking_id)
                
                lat = parking.get('latitude')
                lng = parking.get('longitude')
                
                if lat is None or lng is None:
                    continue
                
                feature = {
                    "type": "Feature",
                    "id": parking_id,
                    "properties": {
                        "id": parking_id,
                        "name": parking.get('name'),
                        "city": city_name,
                        "type": "parking",
                        "capacity": parking.get('capacity'),
                        "address": parking.get('address')
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lng, lat]  # GeoJSON: [lng, lat]
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
    
    print(f"📦 Создан GeoJSON парковок: {output_path} ({len(features)} парковок)")


def convert_vehicles_to_geojson(all_parkings_data, output_path):
    """Конвертация транспорта в GeoJSON."""
    features = []
    seen_vehicle_ids = set()
    
    for city_data in all_parkings_data:
        city_name = city_data['city_name']
        
        for zone_data in city_data.get('zones', []):
            transports_data = zone_data.get('transports', {}).get('data', {})
            vehicles = transports_data.get('transports', [])
            
            for vehicle in vehicles:
                vehicle_id = vehicle.get('id')
                
                # Избегаем дубликатов транспорта
                if vehicle_id in seen_vehicle_ids:
                    continue
                seen_vehicle_ids.add(vehicle_id)
                
                lat = vehicle.get('latitude')
                lng = vehicle.get('longitude')
                
                if lat is None or lng is None:
                    continue
                
                feature = {
                    "type": "Feature",
                    "id": vehicle_id,
                    "properties": {
                        "id": vehicle_id,
                        "number": vehicle.get('number'),
                        "city": city_name,
                        "type": "vehicle",
                        "vehicleType": vehicle.get('type'),
                        "battery": vehicle.get('battery'),
                        "model": vehicle.get('model'),
                        "status": vehicle.get('status')
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lng, lat]  # GeoJSON: [lng, lat]
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
    
    print(f"📦 Создан GeoJSON транспорта: {output_path} ({len(features)} единиц)")


def main():
    print("🚀 Начинаю загрузку парковок и транспорта...\n")
    
    # Загрузка токена
    token = load_config()
    
    # Получение списка городов
    cities = get_cities()
    
    # Структура для хранения всех данных
    all_parkings_data = []
    
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    
    # Обработка каждого города
    for i, city in enumerate(cities, 1):
        city_id = city['id']
        city_name = city['name']
        
        print(f"\n[{i}/{len(cities)}] 🏙️  {city_name}")
        
        city_data = {
            'city_id': city_id,
            'city_name': city_name,
            'zones': []
        }
        
        # Загрузка rent zones
        try:
            print(f"  📥 Загружаю rent zones...")
            rent_zones_data = fetch_rent_zones(city_id, token)
            rent_zones = rent_zones_data.get('data', [])
            print(f"  ✅ Rent zones: {len(rent_zones)}")
            
        except Exception as e:
            print(f"  ⚠️  Ошибка при загрузке rent zones: {e}")
            continue
        
        # Загрузка транспорта для каждой rent zone
        if rent_zones:
            print(f"  📥 Загружаю транспорт (радиус 10км)...")
            
            total_parkings = 0
            total_vehicles = 0
            
            for rent_zone in rent_zones:
                rent_zone_id = rent_zone['id']
                rent_zone_name = rent_zone.get('name', 'Unnamed')
                coordinates = rent_zone.get('coordinates', [])
                
                if not coordinates:
                    continue
                
                # Вычисляем центр зоны
                center = calculate_center(coordinates)
                if not center:
                    continue
                
                try:
                    transports_data = fetch_transports(center, token, radius_meters=10000)
                    
                    # Подсчёт парковок и транспорта
                    data = transports_data.get('data', {})
                    parkings = data.get('parkingList', [])
                    vehicles = data.get('transports', [])
                    
                    total_parkings += len(parkings)
                    total_vehicles += len(vehicles)
                    
                    city_data['zones'].append({
                        'rent_zone_id': rent_zone_id,
                        'rent_zone_name': rent_zone_name,
                        'center': center,
                        'transports': transports_data
                    })
                    
                    # Сохранение в tmp
                    transports_path = tmp_dir / f'transports_{rent_zone_id}.json'
                    save_json(transports_data, transports_path)
                    
                except Exception as e:
                    print(f"  ⚠️  Ошибка для rent zone {rent_zone_name}: {e}")
                    continue
            
            print(f"  ✅ Парковок: {total_parkings}, Транспорта: {total_vehicles}")
        
        all_parkings_data.append(city_data)
    
    # Сохранение объединённых данных
    print("\n💾 Сохраняю данные...")
    all_data_path = tmp_dir / 'all_parkings.json'
    save_json(all_parkings_data, all_data_path)
    print(f"💾 Сохранено: {all_data_path}")
    
    # Конвертация в GeoJSON
    print("\n📍 Конвертирую в GeoJSON...")
    output_dir = base_dir / 'output'
    
    # Парковки
    parkings_geojson_path = output_dir / 'parkings.geojson'
    convert_parkings_to_geojson(all_parkings_data, parkings_geojson_path)
    
    # Транспорт
    vehicles_geojson_path = output_dir / 'vehicles.geojson'
    convert_vehicles_to_geojson(all_parkings_data, vehicles_geojson_path)
    
    print("\n✅ Готово!")
    print(f"   Обработано городов: {len(all_parkings_data)}")
    print("\n⚠️  Примечание: радиус 10км может не покрывать все парковки и транспорт.")
    print("   Это первая версия, в будущем можно будет улучшить покрытие.")


if __name__ == '__main__':
    main()
