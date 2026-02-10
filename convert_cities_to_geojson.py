#!/usr/bin/env python3
"""
Скрипт для конвертации cities.json в all_zones.geojson
Автоматически загружает данные из API если файл отсутствует или устарел
"""
import json
import os
import time
import requests
import sys
from pathlib import Path
from datetime import datetime, timedelta


from datetime import datetime, timedelta


def load_config() -> dict:
    """Загружает конфигурацию из config.json или переменных окружения"""
    config = {}
    
    config_file = Path('config.json')
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            print("✅ Конфигурация загружена из config.json")
    
    if os.getenv('URENT_TOKEN'):
        config['bearer_token'] = os.getenv('URENT_TOKEN')
        print("✅ Токен загружен из переменной окружения URENT_TOKEN")
    
    return config


def is_file_old(filepath: Path, max_age_days: int = 5) -> bool:
    """Проверяет, старше ли файл указанного количества дней"""
    if not filepath.exists():
        return True
    
    file_mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
    age = datetime.now() - file_mtime
    return age > timedelta(days=max_age_days)


def fetch_cities_list(bearer_token: str = None) -> dict:
    """Запрашивает список всех городов из API"""
    url = "https://backyard.urentbike.ru/gatewayclient/api/v3/zones/uses"
    
    lat, lng = 55.77545546986907, 37.63290022965542
    
    params = {
        'availableCityTypes': ['available', 'frozen'],
        'locationLat': lat,
        'locationLng': lng
    }
    
    default_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjhDMUUyQ0JDQUMwNzFFNUVDMkIzMzRBN0Y1RDdERDVCRDY3RDY2NzVSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImpCNHN2S3dISGw3Q3N6U245ZGZkVzlaOVpuVSJ9.eyJuYmYiOjE3NzAzMjY3NjEsImV4cCI6MTc3MDM0MTE2MSwiaXNzIjoiaHR0cHM6Ly9iYWNreWFyZC51cmVudGJpa2UucnUvaWRlbnRpdHkiLCJhdWQiOlsiaWRlbnRpdHkuYXBpIiwiYmlrZS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJjdXN0b21lcnMuYXBpIiwib3JkZXJpbmcuYXBpIiwib3JkZXJpbmcuc2Nvb3Rlci5hcGkiLCJwYXltZW50LmFwaSIsImxvZy5hcGkiLCJkcml2ZXIuYmlrZS5sb2NrLnRvbXNrLmFwaSIsIm1haW50ZW5hbmNlLmFwaSIsIm5vdGlmaWNhdGlvbi5hcGkiLCJtYXJrZXRpbmcuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5zY29vdGVyLm5pbmVib3QuYXBpIiwiZnJhdWQuYW5hbHl6ZS5hcGkiXSwiY2xpZW50X2lkIjoibW9iaWxlLmNsaWVudC5pb3MiLCJzdWIiOiI2MjVhYTg3MDY0N2FkMDc3NTU2NGFhZGQiLCJhdXRoX3RpbWUiOjE3NzAzMjY3NTcsImlkcCI6ImxvY2FsIiwicm9sZSI6IkNMSUVOVCIsInBob25lX251bWJlciI6Ijc5MDU1NDQ3NTE4IiwicGhvbmUiOiI3OTA1NTQ0NzUxOCIsInBsYWNlLmNvZGUiOiJSVSIsInBsYWNlLmN1c3RvbWVyQXBpVXJsIjoiaHR0cHM6Ly9zZXJ2aWNlLnVyZW50YmlrZS5ydS9jdXN0b21lcnMiLCJwbGFjZS5jb3VudHJ5IjoicnVzIiwicGxhY2UuY3VsdHVyZSI6InJ1LVJVIiwiYnJhbmQuY29kZSI6IlVSRU5UIiwibmFtZSI6Ijc5MDU1NDQ3NTE4IiwiQWRtaW5DbGFpbXMiOiIiLCJqdGkiOiJGMkRDMjY3QkQ3QzRCMkYwMjFDRkY4NjZCNDg2MzM2MiIsImlhdCI6MTc3MDMyNjc2MSwic2NvcGUiOlsiYmlrZS5hcGkiLCJjdXN0b21lcnMuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5iaWtlLmxvY2sudG9tc2suYXBpIiwiZHJpdmVyLnNjb290ZXIubmluZWJvdC5hcGkiLCJpZGVudGl0eS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJsb2cuYXBpIiwibWFpbnRlbmFuY2UuYXBpIiwibm90aWZpY2F0aW9uLmFwaSIsIm9yZGVyaW5nLmFwaSIsIm9yZGVyaW5nLnNjb290ZXIuYXBpIiwicGF5bWVudC5hcGkiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsiY3VzdG9tIl19.hGNlpE93FaCEWo0rbArQ7a293E7Qh3vJt6xWmAj3knTaoOLmU9B5igVzVQ0BAd0FK8GAFbkHmLoS_c8oul4OgMZfEK3-rIRGhlsCZBVoHh3CIe_zdcUuC0DvXJOn9-3Cz_0nh6afzixymO2MQqNuBJCUfZT0Nq4y3Y0aNVa9GiMoMqjKjWXdi49NwajUeftNQyulT5MQ6aRiJ7zfplzL6Mz5Bz-Py_VYg7J8vekOHyqlQECZ-zCyIFw4c_RV5i0hYqRu646gfSSeJxXvQ_E_YCnphSipN2OfE0iD4oXBDskLQNVQXnqz8ao_FXue_MaLYIzPMBetEVC_v3wYi6z8679z1VQtqzYxeD0YtfnRDEqBEZjBhSa-H1Eq_P3YH0kKijs2c6q63lJ8CUqMNV7wV-HII5_RZDA6Al7k8tDjn-JFLJk2CYYTti5VBg8b055mpS05AZPmrExOkbhopnjH9AWuZWl3ObrbZx3I6iNTxRKFQo_0oDeFI7L0Udsc9RsySpqKQN-4l1mTXtm7pC-5xRAGJtMSW1pdoGbScbu3GnS_y8WiPqkA0jqih6YqvCLCA_sbvXpjkcxSOWUAgSlq0QRrYRWFwOwRUzww1p_ILCqv8T2sseojEACthy4lQsNR9MhgeCWLuYKFBly-3vL3cKg_jWMuEPvn6suv6x_iEJ4'
    
    token = bearer_token if bearer_token else default_token
    
    headers = {
        'Host': 'backyard.urentbike.ru',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Platform': 'iOS'
    }
    
    try:
        print("🔄 Запрос списка городов из API...")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Получено зон: {len(data.get('data', []))}")
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"❌ Ошибка авторизации (403 Forbidden)")
            print(f"💡 Токен устарел. Обновите токен в config.json")
        else:
            print(f"❌ HTTP ошибка: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при запросе: {e}")
        sys.exit(1)


def convert_to_geojson(zones_data: list) -> dict:
    """Конвертирует массив зон в GeoJSON FeatureCollection"""
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for zone in zones_data:
        # Конвертируем координаты из {lat, lng} в [lng, lat]
        coordinates = []
        for coord in zone.get('coordinates', []):
            if isinstance(coord, dict) and 'lat' in coord and 'lng' in coord:
                coordinates.append([coord['lng'], coord['lat']])
        
        if not coordinates:
            continue
        
        # Создаём GeoJSON Feature
        feature = {
            "type": "Feature",
            "id": zone.get('id'),
            "properties": {
                "id": zone.get('id'),
                "cityId": zone.get('cityId'),
                "cityAvailabilityStatus": zone.get('cityAvailabilityStatus'),
                "center": zone.get('center'),
                "modalities": zone.get('modalities', [])
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coordinates]
            }
        }
        
        geojson['features'].append(feature)
    
    return geojson


def save_json(data: dict, filename: str):
    """Сохраняет JSON в файл"""
    filepath = Path(filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Сохранено: {filepath}")


def main():
    """Основная функция"""
    print(f"\n{'='*70}")
    print(f"🗺️  Конвертация cities.json в all_zones.geojson")
    print(f"{'='*70}\n")
    
    input_file = Path('cities.json')
    
    # Проверяем наличие и возраст файла
    file_exists = input_file.exists()
    file_is_old = is_file_old(input_file, max_age_days=5)
    
    if file_exists:
        file_mtime = datetime.fromtimestamp(input_file.stat().st_mtime)
        age = datetime.now() - file_mtime
        age_days = age.days
        age_hours = age.seconds // 3600
        
        print(f"📂 Файл {input_file} найден")
        print(f"� Возраст файла: {age_days} дней {age_hours} часов")
    
    # Загружаем данные
    if not file_exists:
        print(f"⚠️  Файл {input_file} не найден")
        print(f"📡 Загружаем данные из API...\n")
        
        config = load_config()
        bearer_token = config.get('bearer_token')
        cities_data = fetch_cities_list(bearer_token)
        
        # Сохраняем загруженные данные
        save_json(cities_data, str(input_file))
        print()
        
    elif file_is_old:
        print(f"⚠️  Файл устарел (старше 5 дней)")
        print(f"📡 Обновляем данные из API...\n")
        
        config = load_config()
        bearer_token = config.get('bearer_token')
        cities_data = fetch_cities_list(bearer_token)
        
        # Сохраняем загруженные данные
        save_json(cities_data, str(input_file))
        print()
        
    else:
        print(f"✅ Файл актуален (младше 5 дней)")
        print(f"📂 Чтение файла {input_file}...\n")
        with open(input_file, 'r', encoding='utf-8') as f:
            cities_data = json.load(f)
    
    zones = cities_data.get('data', [])
    
    if not zones:
        print("❌ Зоны не найдены в файле!")
        return
    
    # Фильтруем только доступные зоны
    available_zones = [zone for zone in zones if zone.get('cityAvailabilityStatus') == 'AVAILABLE']
    frozen_zones = [zone for zone in zones if zone.get('cityAvailabilityStatus') == 'FROZEN']
    
    print(f"📊 Статистика:")
    print(f"  • Всего зон: {len(zones)}")
    print(f"  • Доступных (AVAILABLE): {len(available_zones)}")
    print(f"  • Замороженных (FROZEN): {len(frozen_zones)}")
    print(f"  • Других: {len(zones) - len(available_zones) - len(frozen_zones)}")
    
    # Конвертируем только доступные зоны
    print(f"\n🔄 Конвертация доступных зон в GeoJSON...")
    geojson = convert_to_geojson(available_zones)
    
    # Сохраняем
    save_json(geojson, 'all_zones.geojson')
    
    # Также конвертируем все зоны (включая FROZEN)
    print(f"\n🔄 Конвертация всех зон в GeoJSON...")
    geojson_all = convert_to_geojson(zones)
    save_json(geojson_all, 'all_zones_including_frozen.geojson')
    
    print(f"\n{'='*70}")
    print(f"✨ Готово!")
    print(f"{'='*70}")
    print(f"\n📁 Созданные файлы:")
    print(f"  • all_zones.geojson - {len(geojson['features'])} доступных зон")
    print(f"  • all_zones_including_frozen.geojson - {len(geojson_all['features'])} всех зон")
    print(f"\n💡 Файл cities.json будет автоматически обновлён через 5 дней")
    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
