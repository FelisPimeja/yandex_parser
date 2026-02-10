#!/usr/bin/env python3
"""
Скрипт для загрузки зон города из API Urent и конвертации в GeoJSON
"""
import json
import requests
import sys
import os
from pathlib import Path


def load_config() -> dict:
    """Загружает конфигурацию из config.json или переменных окружения"""
    config = {}
    
    # Пытаемся загрузить из файла config.json
    config_file = Path('config.json')
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            print("✅ Конфигурация загружена из config.json")
    
    # Переменные окружения имеют приоритет
    if os.getenv('URENT_TOKEN'):
        config['bearer_token'] = os.getenv('URENT_TOKEN')
        print("✅ Токен загружен из переменной окружения URENT_TOKEN")
    
    return config


def fetch_city_zones(city_id: str, lat: float = 55.77545546986907, lng: float = 37.63290022965542, bearer_token: str = None) -> dict:
    """
    Запрашивает зоны города по ID из API Urent
    
    Args:
        city_id: ID города для запроса
        lat: Широта для запроса (по умолчанию Москва)
        lng: Долгота для запроса (по умолчанию Москва)
        bearer_token: Bearer токен для авторизации (опционально)
    
    Returns:
        dict: JSON ответ от API
    """
    url = f"https://backyard.urentbike.ru/gatewayclient/api/v3/zones/rent"
    
    # Параметры запроса
    params = {
        'cityId': city_id,
        'locationLat': lat,
        'locationLng': lng,
        'useZoneId': city_id
    }
    
    # Токен по умолчанию (может быть устаревшим)
    default_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjhDMUUyQ0JDQUMwNzFFNUVDMkIzMzRBN0Y1RDdERDVCRDY3RDY2NzVSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImpCNHN2S3dISGw3Q3N6U245ZGZkVzlaOVpuVSJ9.eyJuYmYiOjE3NzAzMjY3NjEsImV4cCI6MTc3MDM0MTE2MSwiaXNzIjoiaHR0cHM6Ly9iYWNreWFyZC51cmVudGJpa2UucnUvaWRlbnRpdHkiLCJhdWQiOlsiaWRlbnRpdHkuYXBpIiwiYmlrZS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJjdXN0b21lcnMuYXBpIiwib3JkZXJpbmcuYXBpIiwib3JkZXJpbmcuc2Nvb3Rlci5hcGkiLCJwYXltZW50LmFwaSIsImxvZy5hcGkiLCJkcml2ZXIuYmlrZS5sb2NrLnRvbXNrLmFwaSIsIm1haW50ZW5hbmNlLmFwaSIsIm5vdGlmaWNhdGlvbi5hcGkiLCJtYXJrZXRpbmcuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5zY29vdGVyLm5pbmVib3QuYXBpIiwiZnJhdWQuYW5hbHl6ZS5hcGkiXSwiY2xpZW50X2lkIjoibW9iaWxlLmNsaWVudC5pb3MiLCJzdWIiOiI2MjVhYTg3MDY0N2FkMDc3NTU2NGFhZGQiLCJhdXRoX3RpbWUiOjE3NzAzMjY3NTcsImlkcCI6ImxvY2FsIiwicm9sZSI6IkNMSUVOVCIsInBob25lX251bWJlciI6Ijc5MDU1NDQ3NTE4IiwicGhvbmUiOiI3OTA1NTQ0NzUxOCIsInBsYWNlLmNvZGUiOiJSVSIsInBsYWNlLmN1c3RvbWVyQXBpVXJsIjoiaHR0cHM6Ly9zZXJ2aWNlLnVyZW50YmlrZS5ydS9jdXN0b21lcnMiLCJwbGFjZS5jb3VudHJ5IjoicnVzIiwicGxhY2UuY3VsdHVyZSI6InJ1LVJVIiwiYnJhbmQuY29kZSI6IlVSRU5UIiwibmFtZSI6Ijc5MDU1NDQ3NTE4IiwiQWRtaW5DbGFpbXMiOiIiLCJqdGkiOiJGMkRDMjY3QkQ3QzRCMkYwMjFDRkY4NjZCNDg2MzM2MiIsImlhdCI6MTc3MDMyNjc2MSwic2NvcGUiOlsiYmlrZS5hcGkiLCJjdXN0b21lcnMuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5iaWtlLmxvY2sudG9tc2suYXBpIiwiZHJpdmVyLnNjb290ZXIubmluZWJvdC5hcGkiLCJpZGVudGl0eS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJsb2cuYXBpIiwibWFpbnRlbmFuY2UuYXBpIiwibm90aWZpY2F0aW9uLmFwaSIsIm9yZGVyaW5nLmFwaSIsIm9yZGVyaW5nLnNjb290ZXIuYXBpIiwicGF5bWVudC5hcGkiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsiY3VzdG9tIl19.hGNlpE93FaCEWo0rbArQ7a293E7Qh3vJt6xWmAj3knTaoOLmU9B5igVzVQ0BAd0FK8GAFbkHmLoS_c8oul4OgMZfEK3-rIRGhlsCZBVoHh3CIe_zdcUuC0DvXJOn9-3Cz_0nh6afzixymO2MQqNuBJCUfZT0Nq4y3Y0aNVa9GiMoMqjKjWXdi49NwajUeftNQyulT5MQ6aRiJ7zfplzL6Mz5Bz-Py_VYg7J8vekOHyqlQECZ-zCyIFw4c_RV5i0hYqRu646gfSSeJxXvQ_E_YCnphSipN2OfE0iD4oXBDskLQNVQXnqz8ao_FXue_MaLYIzPMBetEVC_v3wYi6z8679z1VQtqzYxeD0YtfnRDEqBEZjBhSa-H1Eq_P3YH0kKijs2c6q63lJ8CUqMNV7wV-HII5_RZDA6Al7k8tDjn-JFLJk2CYYTti5VBg8b055mpS05AZPmrExOkbhopnjH9AWuZWl3ObrbZx3I6iNTxRKFQo_0oDeFI7L0Udsc9RsySpqKQN-4l1mTXtm7pC-5xRAGJtMSW1pdoGbScbu3GnS_y8WiPqkA0jqih6YqvCLCA_sbvXpjkcxSOWUAgSlq0QRrYRWFwOwRUzww1p_ILCqv8T2sseojEACthy4lQsNR9MhgeCWLuYKFBly-3vL3cKg_jWMuEPvn6suv6x_iEJ4'
    
    token = bearer_token if bearer_token else default_token
    
    # Заголовки запроса
    headers = {
        'Host': 'backyard.urentbike.ru',
        'traceparent': '00-ffae35b6cf6e718ad1ea85322e7efea3-09cc12325b6582ce-01',
        'UR-Session': '625aa870647ad0775564aadd',
        'UR-OS': '26.2.0',
        'UR-Flagr-Experiment-Keys': 'old_cancel_2',
        'UR-Device-Region': 'RU',
        'Environment-Info': 'plt:ios,1.89.0,mod:iPad Pro (12.9-inch) (3rd generation),os:26.2,phone:79055447518',
        'charset': 'UTF-8',
        'UR-Request-Data': 'CE87EC6558AE9FE193CA8626A7F8ABB9520AC8BD6C934D6FE9C0792F0DCB3A58',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'ur-request-version': 'v2',
        'Accept': '*/*',
        'UR-Longitude': str(lng),
        'UR-Client-Id': 'mobile.client.ios',
        'UR-User-Id': '625aa870647ad0775564aadd',
        'UR-Latitude': str(lat),
        'X-AppsFlyer-Id': '1770274489767-6693582',
        'Connection': 'keep-alive',
        'X-AppsFlyer-Idfa': '',
        'UR-Device-Id': '669D35D8-A248-527D-B804-5A998D6724C9',
        'Accept-Language': 'en-US',
        'Authorization': f'Bearer {token}',
        'UR-Time-Zone': 'GMT+3',
        'UR-Brand': 'URENT',
        'Content-Type': 'application/json',
        'UR-Device-Model': 'iPad Pro (12.9-inch) (3rd generation)',
        'UR-Version': '1.89.0',
        'Accept-Encoding': 'gzip',
        'UR-Platform': 'iOS'
    }
    
    try:
        print(f"🔄 Запрос зон для города ID: {city_id}...")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Получен ответ: {response.status_code}")
        return data
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"❌ Ошибка авторизации (403 Forbidden)")
            print(f"💡 Токен устарел. Обновите токен в config.json или через переменную окружения URENT_TOKEN")
            print(f"📖 Подробнее см. README.md")
        else:
            print(f"❌ HTTP ошибка: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при запросе: {e}")
        sys.exit(1)


def convert_to_geojson(data: dict) -> dict:
    """
    Конвертирует данные из формата Urent API в GeoJSON
    
    Args:
        data: JSON ответ от API Urent
    
    Returns:
        dict: GeoJSON FeatureCollection
    """
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Получаем массив зон из data
    zones = data.get('data', [])
    
    if not zones:
        print("⚠️  Зоны не найдены в ответе API")
        return geojson
    
    # Конвертируем каждую зону в GeoJSON Feature
    for zone in zones:
        # Конвертируем координаты из {lat, lng} в [lng, lat] (формат GeoJSON)
        coordinates = []
        for coord in zone.get('coordinates', []):
            if isinstance(coord, dict) and 'lat' in coord and 'lng' in coord:
                coordinates.append([coord['lng'], coord['lat']])
        
        if not coordinates:
            print(f"⚠️  Зона {zone.get('id')} не имеет валидных координат, пропускаем")
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
    
    print(f"✅ Конвертировано зон: {len(geojson['features'])}")
    return geojson


def save_json(data: dict, filename: str):
    """Сохраняет JSON в файл"""
    filepath = Path(filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Сохранено: {filepath}")


def main():
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Использование: python fetch_city_zones.py <city_id> [output_name]")
        print("\nПримеры:")
        print("  python fetch_city_zones.py 64307422f158b50245fdae7d")
        print("  python fetch_city_zones.py 64307422f158b50245fdae7d maykop")
        sys.exit(1)
    
    city_id = sys.argv[1]
    output_name = sys.argv[2] if len(sys.argv) > 2 else city_id
    
    print(f"\n{'='*60}")
    print(f"🏙️  Загрузка и конвертация зон города")
    print(f"{'='*60}\n")
    
    # Загружаем конфигурацию
    config = load_config()
    bearer_token = config.get('bearer_token')
    
    # Запрашиваем данные из API
    data = fetch_city_zones(city_id, bearer_token=bearer_token)
    
    # Сохраняем исходный JSON
    save_json(data, f"{output_name}.json")
    
    # Конвертируем в GeoJSON
    print(f"\n🔄 Конвертация в GeoJSON...")
    geojson = convert_to_geojson(data)
    
    # Сохраняем GeoJSON
    save_json(geojson, f"{output_name}.geojson")
    
    print(f"\n{'='*60}")
    print(f"✨ Готово!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
