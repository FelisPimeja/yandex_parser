#!/usr/bin/env python3
"""
Скрипт для загрузки парковок и транспорта для Сочи.
Тестовая версия для одного города.

Использование:
    python3 fetch_parkings_sochi.py
"""

import json
import os
import sys
from pathlib import Path
import requests


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


def calculate_center(coordinates):
    """Вычисление центра полигона."""
    if not coordinates:
        return None
    
    lats = [point[0] for point in coordinates]
    lngs = [point[1] for point in coordinates]
    
    center_lat = sum(lats) / len(lats)
    center_lng = sum(lngs) / len(lngs)
    
    return {
        "latitude": center_lat,
        "longitude": center_lng
    }


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
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 403:
        print("❌ Ошибка 403: Токен истёк или недействителен")
        print("Обновите токен в config.json")
        sys.exit(1)
    
    response.raise_for_status()
    return response.json()


def fetch_transports(center, token, radius_meters=10000):
    """Загрузка доступного транспорта в радиусе от центра."""
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
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def save_json(data, output_path):
    """Сохранение данных в JSON файл."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def convert_parkings_to_geojson(parkings_data, output_path):
    """Конвертация парковок в GeoJSON."""
    features = []
    seen_parking_ids = set()
    
    for zone_data in parkings_data:
        transports_data = zone_data.get('transports', {}).get('data', {})
        parkings = transports_data.get('parkingList', [])
        
        for parking in parkings:
            parking_id = parking.get('id')
            
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
                    "city": "Сочи",
                    "type": "parking",
                    "capacity": parking.get('capacity'),
                    "address": parking.get('address')
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
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


def convert_vehicles_to_geojson(parkings_data, output_path):
    """Конвертация транспорта в GeoJSON."""
    features = []
    seen_vehicle_ids = set()
    
    for zone_data in parkings_data:
        transports_data = zone_data.get('transports', {}).get('data', {})
        vehicles = transports_data.get('transports', [])
        
        for vehicle in vehicles:
            vehicle_id = vehicle.get('id')
            
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
                    "city": "Сочи",
                    "type": "vehicle",
                    "vehicleType": vehicle.get('type'),
                    "battery": vehicle.get('battery'),
                    "model": vehicle.get('model'),
                    "status": vehicle.get('status')
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
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
    print("🚀 Загрузка парковок и транспорта для Сочи...\n")
    
    # ID Сочи
    SOCHI_CITY_ID = "5f96dd383719ad000142ba5d"
    
    # Загрузка токена
    token = load_config()
    
    # Структура для хранения данных
    parkings_data = []
    
    base_dir = Path(__file__).parent
    tmp_dir = base_dir / 'output' / 'tmp'
    
    print(f"🏙️  Сочи")
    
    # Загрузка rent zones
    try:
        print(f"  📥 Загружаю rent zones...")
        rent_zones_response = fetch_rent_zones(SOCHI_CITY_ID, token)
        rent_zones = rent_zones_response.get('data', [])
        print(f"  ✅ Rent zones: {len(rent_zones)}")
        
        # Сохранение rent zones
        rent_zones_path = tmp_dir / f'rent_zones_sochi.json'
        save_json(rent_zones_response, rent_zones_path)
        print(f"  💾 Сохранено: {rent_zones_path}")
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке rent zones: {e}")
        sys.exit(1)
    
    # Загрузка транспорта для каждой rent zone
    if rent_zones:
        print(f"\n  📥 Загружаю транспорт (радиус 10км)...")
        
        total_parkings = 0
        total_vehicles = 0
        
        for i, rent_zone in enumerate(rent_zones, 1):
            rent_zone_id = rent_zone['id']
            rent_zone_name = rent_zone.get('name', 'Unnamed')
            coordinates = rent_zone.get('coordinates', [])
            
            if not coordinates:
                print(f"  ⚠️  [{i}/{len(rent_zones)}] {rent_zone_name}: нет координат")
                continue
            
            # Вычисляем центр зоны
            center = calculate_center(coordinates)
            if not center:
                print(f"  ⚠️  [{i}/{len(rent_zones)}] {rent_zone_name}: не удалось вычислить центр")
                continue
            
            try:
                print(f"  🔄 [{i}/{len(rent_zones)}] {rent_zone_name}: центр ({center['latitude']:.4f}, {center['longitude']:.4f})")
                transports_response = fetch_transports(center, token, radius_meters=10000)
                
                # Подсчёт парковок и транспорта
                data = transports_response.get('data', {})
                parkings = data.get('parkingList', [])
                vehicles = data.get('transports', [])
                
                print(f"     ✅ Парковок: {len(parkings)}, Транспорта: {len(vehicles)}")
                
                total_parkings += len(parkings)
                total_vehicles += len(vehicles)
                
                parkings_data.append({
                    'rent_zone_id': rent_zone_id,
                    'rent_zone_name': rent_zone_name,
                    'center': center,
                    'transports': transports_response
                })
                
                # Сохранение в tmp
                transports_path = tmp_dir / f'transports_sochi_{rent_zone_id}.json'
                save_json(transports_response, transports_path)
                
            except Exception as e:
                print(f"     ❌ Ошибка: {e}")
                continue
        
        print(f"\n  📊 Итого для Сочи:")
        print(f"     Парковок: {total_parkings}")
        print(f"     Транспорта: {total_vehicles}")
    
    # Сохранение объединённых данных
    print("\n💾 Сохраняю данные...")
    all_data_path = tmp_dir / 'parkings_sochi.json'
    save_json(parkings_data, all_data_path)
    print(f"💾 Сохранено: {all_data_path}")
    
    # Конвертация в GeoJSON
    print("\n📍 Конвертирую в GeoJSON...")
    output_dir = base_dir / 'output'
    
    # Парковки
    parkings_geojson_path = output_dir / 'parkings_sochi.geojson'
    convert_parkings_to_geojson(parkings_data, parkings_geojson_path)
    
    # Транспорт
    vehicles_geojson_path = output_dir / 'vehicles_sochi.geojson'
    convert_vehicles_to_geojson(parkings_data, vehicles_geojson_path)
    
    print("\n✅ Готово!")
    print("\n⚠️  Примечание: радиус 10км может не покрывать все парковки и транспорт.")
    print("   Это первая версия, в будущем можно будет улучшить покрытие.")


if __name__ == '__main__':
    main()
