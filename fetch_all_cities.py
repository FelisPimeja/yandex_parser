#!/usr/bin/env python3
"""
Скрипт для загрузки списка всех городов из API Urent
"""
import json
import requests
import sys
import os
from pathlib import Path


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


def fetch_cities_list(lat: float = 55.77545546986907, lng: float = 37.63290022965542, bearer_token: str = None) -> dict:
    """
    Запрашивает список всех городов из API Urent
    
    Args:
        lat: Широта для запроса
        lng: Долгота для запроса
        bearer_token: Bearer токен для авторизации
    
    Returns:
        dict: JSON ответ от API со списком городов
    """
    url = "https://backyard.urentbike.ru/gatewayclient/api/v3/zones/uses"
    
    params = {
        'availableCityTypes': ['available', 'frozen'],
        'locationLat': lat,
        'locationLng': lng
    }
    
    default_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjhDMUUyQ0JDQUMwNzFFNUVDMkIzMzRBN0Y1RDdERDVCRDY3RDY2NzVSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImpCNHN2S3dISGw3Q3N6U245ZGZkVzlaOVpuVSJ9.eyJuYmYiOjE3NzAzMjY3NjEsImV4cCI6MTc3MDM0MTE2MSwiaXNzIjoiaHR0cHM6Ly9iYWNreWFyZC51cmVudGJpa2UucnUvaWRlbnRpdHkiLCJhdWQiOlsiaWRlbnRpdHkuYXBpIiwiYmlrZS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJjdXN0b21lcnMuYXBpIiwib3JkZXJpbmcuYXBpIiwib3JkZXJpbmcuc2Nvb3Rlci5hcGkiLCJwYXltZW50LmFwaSIsImxvZy5hcGkiLCJkcml2ZXIuYmlrZS5sb2NrLnRvbXNrLmFwaSIsIm1haW50ZW5hbmNlLmFwaSIsIm5vdGlmaWNhdGlvbi5hcGkiLCJtYXJrZXRpbmcuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5zY29vdGVyLm5pbmVib3QuYXBpIiwiZnJhdWQuYW5hbHl6ZS5hcGkiXSwiY2xpZW50X2lkIjoibW9iaWxlLmNsaWVudC5pb3MiLCJzdWIiOiI2MjVhYTg3MDY0N2FkMDc3NTU2NGFhZGQiLCJhdXRoX3RpbWUiOjE3NzAzMjY3NTcsImlkcCI6ImxvY2FsIiwicm9sZSI6IkNMSUVOVCIsInBob25lX251bWJlciI6Ijc5MDU1NDQ3NTE4IiwicGhvbmUiOiI3OTA1NTQ0NzUxOCIsInBsYWNlLmNvZGUiOiJSVSIsInBsYWNlLmN1c3RvbWVyQXBpVXJsIjoiaHR0cHM6Ly9zZXJ2aWNlLnVyZW50YmlrZS5ydS9jdXN0b21lcnMiLCJwbGFjZS5jb3VudHJ5IjoicnVzIiwicGxhY2UuY3VsdHVyZSI6InJ1LVJVIiwiYnJhbmQuY29kZSI6IlVSRU5UIiwibmFtZSI6Ijc5MDU1NDQ3NTE4IiwiQWRtaW5DbGFpbXMiOiIiLCJqdGkiOiJGMkRDMjY3QkQ3QzRCMkYwMjFDRkY4NjZCNDg2MzM2MiIsImlhdCI6MTc3MDMyNjc2MSwic2NvcGUiOlsiYmlrZS5hcGkiLCJjdXN0b21lcnMuYXBpIiwiZHJpdmVyLmJpa2UubG9jay5vZmZvLmFwaSIsImRyaXZlci5iaWtlLmxvY2sudG9tc2suYXBpIiwiZHJpdmVyLnNjb290ZXIubmluZWJvdC5hcGkiLCJpZGVudGl0eS5hcGkiLCJsb2NhdGlvbi5hcGkiLCJsb2cuYXBpIiwibWFpbnRlbmFuY2UuYXBpIiwibm90aWZpY2F0aW9uLmFwaSIsIm9yZGVyaW5nLmFwaSIsIm9yZGVyaW5nLnNjb290ZXIuYXBpIiwicGF5bWVudC5hcGkiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsiY3VzdG9tIl19.hGNlpE93FaCEWo0rbArQ7a293E7Qh3vJt6xWmAj3knTaoOLmU9B5igVzVQ0BAd0FK8GAFbkHmLoS_c8oul4OgMZfEK3-rIRGhlsCZBVoHh3CIe_zdcUuC0DvXJOn9-3Cz_0nh6afzixymO2MQqNuBJCUfZT0Nq4y3Y0aNVa9GiMoMqjKjWXdi49NwajUeftNQyulT5MQ6aRiJ7zfplzL6Mz5Bz-Py_VYg7J8vekOHyqlQECZ-zCyIFw4c_RV5i0hYqRu646gfSSeJxXvQ_E_YCnphSipN2OfE0iD4oXBDskLQNVQXnqz8ao_FXue_MaLYIzPMBetEVC_v3wYi6z8679z1VQtqzYxeD0YtfnRDEqBEZjBhSa-H1Eq_P3YH0kKijs2c6q63lJ8CUqMNV7wV-HII5_RZDA6Al7k8tDjn-JFLJk2CYYTti5VBg8b055mpS05AZPmrExOkbhopnjH9AWuZWl3ObrbZx3I6iNTxRKFQo_0oDeFI7L0Udsc9RsySpqKQN-4l1mTXtm7pC-5xRAGJtMSW1pdoGbScbu3GnS_y8WiPqkA0jqih6YqvCLCA_sbvXpjkcxSOWUAgSlq0QRrYRWFwOwRUzww1p_ILCqv8T2sseojEACthy4lQsNR9MhgeCWLuYKFBly-3vL3cKg_jWMuEPvn6suv6x_iEJ4'
    
    token = bearer_token if bearer_token else default_token
    
    headers = {
        'Host': 'backyard.urentbike.ru',
        'traceparent': '00-96f14e48b2e9b2d3f7db8d78f0a78938-9effc793b77a7175-01',
        'UR-Device-Model': 'iPad Pro (12.9-inch) (3rd generation)',
        'UR-OS': '26.2.0',
        'UR-Flagr-Experiment-Keys': 'old_cancel_2',
        'UR-Device-Region': 'RU',
        'Environment-Info': 'plt:ios,1.89.0,mod:iPad Pro (12.9-inch) (3rd generation),os:26.2,phone:79055447518',
        'charset': 'UTF-8',
        'UR-Request-Data': '3E33C904C1F7C5172FCFE602D634386FEF0F54F385289881FFCD579D490453BA',
        'User-Agent': 'Urent/1.89.0 (ru.urentbike.app; build:8; iOS)',
        'ur-request-version': 'v2',
        'Accept': '*/*',
        'UR-Longitude': str(lng),
        'UR-Client-Id': 'mobile.client.ios',
        'UR-Session': '625aa870647ad0775564aadd',
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
        'UR-Version': '1.89.0',
        'Accept-Encoding': 'gzip',
        'UR-User-Id': '625aa870647ad0775564aadd',
        'UR-Platform': 'iOS'
    }
    
    try:
        print(f"🔄 Запрос списка городов...")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Получен ответ: {response.status_code}")
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


def save_json(data: dict, filename: str):
    """Сохраняет JSON в файл"""
    filepath = Path(filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Сохранено: {filepath}")


def main():
    """Основная функция"""
    print(f"\n{'='*60}")
    print(f"🌍 Загрузка списка всех городов Urent")
    print(f"{'='*60}\n")
    
    # Загружаем конфигурацию
    config = load_config()
    bearer_token = config.get('bearer_token')
    
    # Запрашиваем данные
    data = fetch_cities_list(bearer_token=bearer_token)
    
    # Сохраняем
    save_json(data, 'all_cities.json')
    
    # Выводим статистику
    cities = data.get('data', [])
    print(f"\n📊 Статистика:")
    print(f"  Всего городов: {len(cities)}")
    
    if cities:
        print(f"\n📍 Примеры городов (первые 10):")
        for i, city in enumerate(cities[:10], 1):
            city_id = city.get('id', 'N/A')
            # Пытаемся найти название города в разных полях
            city_name = city.get('name') or city.get('cityName') or 'Без названия'
            status = city.get('cityAvailabilityStatus', 'UNKNOWN')
            print(f"  {i}. {city_name} ({status})")
            print(f"     ID: {city_id}")
    
    print(f"\n{'='*60}")
    print(f"✨ Готово!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
