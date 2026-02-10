#!/usr/bin/env python3
"""
Скрипт для загрузки всех RESTRICTION ZONES для всех rent zones

Алгоритм:
1. Загружает список городов через /zones/uses
2. Для каждого AVAILABLE города загружает rent zones через /zones/rent
3. Для каждой rent zone загружает restriction zones через /zones/general
4. Объединяет все restriction zones в GeoJSON файлы по типам
"""
import json
import requests
import sys
import os
import time
from pathlib import Path
from typing import Optional, List, Dict


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


def fetch_cities_list(bearer_token: Optional[str] = None) -> dict:
    """Запрашивает список всех городов (city boundaries)"""
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
        print("🔄 Запрос списка городов...")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Получено: {len(data.get('data', []))} city boundaries")
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


def fetch_city_rent_zones(city_id: str, bearer_token: Optional[str] = None, retry_count: int = 3) -> Optional[dict]:
    """Запрашивает rent zones конкретного города через API /zones/rent"""
    url = "https://backyard.urentbike.ru/gatewayclient/api/v3/zones/rent"
    
    lat, lng = 55.77545546986907, 37.63290022965542
    
    params = {
        'cityId': city_id,
        'locationLat': lat,
        'locationLng': lng,
        'useZoneId': city_id
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
    
    for attempt in range(retry_count):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                wait_time = (attempt + 1) * 2
                time.sleep(wait_time)
            else:
                return None
    
    return None


def fetch_rent_zone_restrictions(rent_zone_id: str, bearer_token: Optional[str] = None, retry_count: int = 3) -> Optional[dict]:
    """Запрашивает restriction zones для конкретной rent zone через API /zones/general"""
    url = "https://backyard.urentbike.ru/gatewayclient/api/v5/zones/general"
    
    lat, lng = 55.77545546986907, 37.63290022965542
    
    params = {
        'rentZoneId': rent_zone_id,
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
    
    for attempt in range(retry_count):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                wait_time = (attempt + 1) * 2
                time.sleep(wait_time)
            else:
                return None
    
    return None


def convert_restriction_zone_to_feature(zone: dict, zone_type: str, rent_zone_id: str) -> Optional[dict]:
    """Конвертирует одну restriction zone в GeoJSON Feature"""
    # Конвертируем координаты из {lat, lng} в [lng, lat]
    coordinates = []
    for coord in zone.get('coordinates', []):
        if isinstance(coord, dict) and 'lat' in coord and 'lng' in coord:
            coordinates.append([coord['lng'], coord['lat']])
    
    # Если нет координат, пропускаем эту зону
    if not coordinates:
        return None
    
    # Создаём свойства для зоны
    properties = {
        "id": zone.get('id'),
        "name": zone.get('name'),
        "type": zone_type,
        "rentZoneId": rent_zone_id  # Добавляем ссылку на родительскую rent zone
    }
    
    # Добавляем center если есть
    if zone.get('center'):
        properties['center'] = zone.get('center')
    
    # Для зон с ограничением скорости добавляем доп. параметры
    if zone_type == 'lowSpeedZone':
        properties['scheduleType'] = zone.get('scheduleType')
        properties['speedLimitValue'] = zone.get('speedLimitValue')
    
    # Создаём GeoJSON Feature
    feature = {
        "type": "Feature",
        "id": zone.get('id'),
        "properties": properties,
        "geometry": {
            "type": "Polygon",
            "coordinates": [coordinates]
        }
    }
    
    return feature


def save_json(data: dict, filename: str):
    """Сохраняет JSON в файл"""
    filepath = Path(filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Сохранено: {filepath}")


def main():
    """Основная функция"""
    print(f"\n{'='*70}")
    print(f"🚦 Загрузка всех RESTRICTION ZONES для всех rent zones")
    print(f"{'='*70}\n")
    
    # Загружаем конфигурацию
    config = load_config()
    bearer_token = config.get('bearer_token')
    
    # Шаг 1: Получаем список городов
    print(f"\n📍 Шаг 1: Загрузка списка городов")
    print(f"{'-'*70}")
    cities_data = fetch_cities_list(bearer_token)
    city_borders = cities_data.get('data', [])
    
    if not city_borders:
        print("❌ Города не найдены!")
        sys.exit(1)
    
    # Получаем уникальные cityId для доступных городов
    city_ids_map = {}
    for border in city_borders:
        city_id = border.get('cityId')
        status = border.get('cityAvailabilityStatus')
        if city_id not in city_ids_map:
            city_ids_map[city_id] = status
    
    # Фильтруем только доступные города
    available_city_ids = [
        city_id for city_id, status in city_ids_map.items() 
        if status == 'AVAILABLE'
    ]
    
    print(f"\n📊 Статистика городов:")
    print(f"  • Всего city boundaries: {len(city_borders)}")
    print(f"  • Уникальных городов: {len(city_ids_map)}")
    print(f"  • Доступных (AVAILABLE): {len(available_city_ids)}")
    
    # Шаг 2: Загружаем rent zones для каждого города
    print(f"\n🗺️  Шаг 2: Загрузка rent zones для каждого города")
    print(f"{'-'*70}")
    
    all_rent_zones = []
    for i, city_id in enumerate(available_city_ids, 1):
        print(f"[{i}/{len(available_city_ids)}] Город {city_id[:12]}...", end=' ')
        
        zones_response = fetch_city_rent_zones(city_id, bearer_token)
        
        if zones_response and zones_response.get('data'):
            zones = zones_response['data']
            all_rent_zones.extend(zones)
            print(f"✅ {len(zones)} rent zones")
        else:
            print(f"⚠️  Ошибка")
        
        time.sleep(0.3)  # Небольшая задержка
    
    print(f"\n📊 Всего rent zones: {len(all_rent_zones)}")
    
    # Шаг 3: Загружаем restriction zones для каждой rent zone
    print(f"\n🚦 Шаг 3: Загрузка restriction zones для каждой rent zone")
    print(f"{'-'*70}")
    print(f"Это займёт время (~{len(all_rent_zones)} запросов)...\n")
    
    all_restrictions = {
        'lowSpeedZones': [],
        'restrictedZones': [],
        'notAllowedZones': []
    }
    
    success_count = 0
    failed_count = 0
    
    for i, rent_zone in enumerate(all_rent_zones, 1):
        rent_zone_id = rent_zone.get('id')
        rent_zone_name = rent_zone.get('name', 'Unknown')
        
        print(f"[{i}/{len(all_rent_zones)}] {rent_zone_name[:30]}...", end=' ')
        
        restrictions_response = fetch_rent_zone_restrictions(rent_zone_id, bearer_token)
        
        if restrictions_response and restrictions_response.get('data'):
            data = restrictions_response['data']
            
            # Подсчитываем зоны
            low_speed = len(data.get('lowSpeedZones', []))
            restricted = len(data.get('restrictedZones', []))
            not_allowed = len(data.get('notAllowedZones', []))
            total = low_speed + restricted + not_allowed
            
            if total > 0:
                print(f"✅ {total} зон (🐌{low_speed} 🚫{restricted} ⛔{not_allowed})")
                
                # Добавляем зоны с привязкой к rent zone
                for zone in data.get('lowSpeedZones', []):
                    feature = convert_restriction_zone_to_feature(zone, 'lowSpeedZone', rent_zone_id)
                    if feature:
                        all_restrictions['lowSpeedZones'].append(feature)
                
                for zone in data.get('restrictedZones', []):
                    feature = convert_restriction_zone_to_feature(zone, 'restrictedZone', rent_zone_id)
                    if feature:
                        all_restrictions['restrictedZones'].append(feature)
                
                for zone in data.get('notAllowedZones', []):
                    feature = convert_restriction_zone_to_feature(zone, 'notAllowedZone', rent_zone_id)
                    if feature:
                        all_restrictions['notAllowedZones'].append(feature)
                
                success_count += 1
            else:
                print(f"✓ нет ограничений")
                success_count += 1
        else:
            print(f"❌")
            failed_count += 1
        
        # Задержка между запросами
        if i < len(all_rent_zones):
            time.sleep(0.4)
    
    # Шаг 4: Создаём GeoJSON файл
    print(f"\n🔄 Шаг 4: Создание GeoJSON файла")
    print(f"{'-'*70}")
    
    # Объединённый файл со всеми restriction zones
    all_features = (
        all_restrictions['lowSpeedZones'] + 
        all_restrictions['restrictedZones'] + 
        all_restrictions['notAllowedZones']
    )
    
    all_geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }
    
    save_json(all_geojson, 'all_restrictions.geojson')
    
    # Итоговая статистика
    print(f"\n{'='*70}")
    print(f"✨ Готово!")
    print(f"{'='*70}")
    print(f"\n📊 Итоговая статистика:")
    print(f"  • Доступных городов: {len(available_city_ids)}")
    print(f"  • Rent zones обработано: {len(all_rent_zones)}")
    print(f"  • Успешно загружено: {success_count}")
    print(f"  • Ошибок: {failed_count}")
    print(f"\n  Restriction zones по типам:")
    print(f"    🐌 Low Speed Zones: {len(all_restrictions['lowSpeedZones'])}")
    print(f"    🚫 Restricted Zones: {len(all_restrictions['restrictedZones'])}")
    print(f"    ⛔ Not Allowed Zones: {len(all_restrictions['notAllowedZones'])}")
    print(f"    📦 Всего: {len(all_features)}")
    
    print(f"\n📁 Созданный файл:")
    print(f"  • all_restrictions.geojson - все restriction zones ({len(all_features)} зон)")
    print(f"\n💡 Каждая зона содержит поле 'type' и 'rentZoneId'")
    print(f"   - type: lowSpeedZone | restrictedZone | notAllowedZone")
    print(f"   - rentZoneId: ID родительской rent zone")
    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
