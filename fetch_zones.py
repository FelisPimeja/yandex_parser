#!/usr/bin/env python3
"""
Скрипт для автоматической загрузки детальных зон (ограничений) для всех городов.

Использует данные из output/cities.geojson (результат fetch_cities.py)
и загружает детальные зоны для каждого города.

Использование:
    python3 fetch_zones.py                     # Все города
    python3 fetch_zones.py --city "Сочи"       # Только указанный город
    python3 fetch_zones.py --continue_from 15  # Продолжить с города N
"""

import json
import os
import sys
import argparse
import time
import csv
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
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    headers = config.get('yandex_headers')
    
    if not headers:
        print("❌ Ошибка: заголовки не найдены в config.json!")
        sys.exit(1)
    
    return headers


def find_cities_by_name(city_name):
    """
    Ищет все зоны города по названию в cities_list.csv.
    Возвращает список словарей с полями: id, name, country, bbox
    """
    cities_csv = Path(__file__).parent / 'cities_list.csv'
    
    if not cities_csv.exists():
        print("❌ Ошибка: файл cities_list.csv не найден!")
        print("Сначала запустите: python3 geocode_cities.py")
        sys.exit(1)
    
    matching_cities = []
    
    with open(cities_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['name'].lower() == city_name.lower():
                matching_cities.append({
                    'id': row['id'],
                    'name': row['name'],
                    'country': row['country'],
                    'bbox': [float(x) for x in row['bbox'].split(',')]
                })
    
    if not matching_cities:
        print(f"❌ Город '{city_name}' не найден в cities_list.csv")
        print("\nДоступные города:")
        
        # Показать первые 10 городов для справки
        with open(cities_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            seen_names = set()
            count = 0
            for row in reader:
                if row['name'] not in seen_names:
                    print(f"  • {row['name']} ({row['country']})")
                    seen_names.add(row['name'])
                    count += 1
                    if count >= 10:
                        print("  ...")
                        break
        sys.exit(1)
    
    return matching_cities


def calculate_polygon_bounds(coordinates):
    """
    Вычисление границ полигона (bbox).
    
    Args:
        coordinates: массив координат полигона [[lon, lat], ...]
    
    Returns:
        list: [min_lon, min_lat, max_lon, max_lat]
    """
    # Полигон может быть многоуровневым (с дырками)
    # Берём первое кольцо (внешний контур)
    if isinstance(coordinates[0][0], list):
        # MultiPolygon или Polygon с дырками
        ring = coordinates[0]
    else:
        ring = coordinates
    
    lons = [coord[0] for coord in ring]
    lats = [coord[1] for coord in ring]
    
    return [min(lons), min(lats), max(lons), max(lats)]


def calculate_polygon_centroid(coordinates):
    """
    Вычисление центроида полигона (упрощённый метод - среднее координат).
    
    Args:
        coordinates: массив координат полигона [[lon, lat], ...]
    
    Returns:
        list: [lon, lat]
    """
    # Берём первое кольцо (внешний контур)
    if isinstance(coordinates[0][0], list):
        ring = coordinates[0]
    else:
        ring = coordinates
    
    lons = [coord[0] for coord in ring]
    lats = [coord[1] for coord in ring]
    
    # Простой центроид - среднее арифметическое
    # Для более точного расчёта нужен weighted centroid, но для наших целей достаточно
    return [sum(lons) / len(lons), sum(lats) / len(lats)]


def simplify_zone_feature(feature, city_polygon_id):
    """
    Упрощение структуры зоны: оставляем только id, city_id, type, speed_limit.
    
    Args:
        feature: dict, GeoJSON feature с зоной
        city_polygon_id: str, ID полигона города
    
    Returns:
        dict: упрощённый GeoJSON feature
    """
    props = feature.get('properties', {})
    
    # Извлекаем zone_type из options
    zone_type = None
    options = props.get('options', [])
    for opt in options:
        for action in opt.get('actions', []):
            if action.get('zone_type'):
                zone_type = action['zone_type']
                break
        if zone_type:
            break
    
    # Извлекаем speed_limit из centroid.style.image.name
    # Пример: "scooters_zone_restrictions_speed_limit_15" -> 15
    speed_limit = None
    centroid = props.get('centroid', {})
    if centroid and zone_type == 'speed_limit':
        style = centroid.get('style', {})
        image = style.get('image', {})
        image_name = image.get('name', '')
        
        # Извлекаем число из конца строки после последнего "_"
        if image_name and 'speed_limit_' in image_name:
            parts = image_name.split('_')
            if parts and parts[-1].isdigit():
                speed_limit = int(parts[-1])
    
    # Создаём упрощённую структуру
    simplified = {
        'id': feature.get('id'),
        'type': 'Feature',
        'geometry': feature.get('geometry'),
        'properties': {
            'city_id': city_polygon_id
        }
    }
    
    # Добавляем type и speed_limit только если они есть
    if zone_type:
        simplified['properties']['type'] = zone_type
    if speed_limit is not None:
        simplified['properties']['speed_limit'] = speed_limit
    
    return simplified


def load_city_polygons(geojson_path):
    """
    Загрузка полигонов городов из cities.geojson.
    
    Returns:
        list of dict: [{id, geometry, bbox, centroid}, ...]
    """
    if not geojson_path.exists():
        print(f"❌ Ошибка: файл {geojson_path} не найден!")
        print("Сначала запустите: python3 fetch_cities.py")
        sys.exit(1)
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cities = []
    
    for feature in data.get('features', []):
        polygon_id = feature.get('id')
        geometry = feature.get('geometry')
        
        if not geometry or geometry.get('type') != 'Polygon':
            continue
        
        coordinates = geometry.get('coordinates')
        if not coordinates:
            continue
        
        try:
            bbox = calculate_polygon_bounds(coordinates)
            centroid = calculate_polygon_centroid(coordinates)
            
            cities.append({
                'id': polygon_id,
                'geometry': geometry,
                'bbox': bbox,  # [min_lon, min_lat, max_lon, max_lat]
                'centroid': centroid  # [lon, lat]
            })
        except Exception as e:
            print(f"⚠️  Пропущен полигон {polygon_id}: {e}")
            continue
    
    return cities


def fetch_city_zones(city_id, location, bbox, zoom=16.7, headers=None):
    """
    Загрузка детальных зон для города.
    
    Args:
        city_id: str, ID полигона города
        location: list [lon, lat]
        bbox: list [min_lon, min_lat, max_lon, max_lat]
        zoom: float
        headers: dict
    
    Returns:
        dict с GeoJSON FeatureCollection или None при ошибке
    """
    endpoint = "/4.0/layers/v1/polygons"
    url = f"{BASE_URL}{endpoint}"
    
    params = {
        "mobcf": "russia%25go_ru_by_geo_hosts_2%25default",
        "mobpr": "go_ru_by_geo_hosts_2_TAXI_V4_0"
    }
    
    data = {
        "state": {
            "location": location,
            "bbox": bbox,
            "zoom": zoom,
            "night_mode": False,  # КРИТИЧНО! Без этого может вернуть пустой ответ
            "screen": "discovery",
            "mode": "scooters",
            "known_orders_info": [],
            "multiclass_options": {"selected": False},
            "scooters": {"autoselect": False},
            "known_orders": []
        },
        "known_versions": {}
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
        
        if response.status_code == 405:
            print(f"      ❌ HTTP 405: JWT токен истёк!")
            return None
        elif response.status_code == 401:
            print(f"      ❌ HTTP 401: Не авторизован")
            return None
        elif response.status_code == 403:
            print(f"      ❌ HTTP 403: Доступ запрещен")
            return None
        
        response.raise_for_status()
        result = response.json()
        
        # Проверяем, что вернулся GeoJSON
        if result.get('type') != 'FeatureCollection':
            print(f"      ⚠️  Неожиданная структура ответа")
            return None
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"      ❌ Ошибка запроса: {e}")
        return None


def save_city_zones(city_id, zones_data, output_dir):
    """Сохранение зон города в отдельный файл с упрощением структуры."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Упрощаем структуру всех зон
    simplified_features = []
    for feature in zones_data.get('features', []):
        simplified = simplify_zone_feature(feature, city_id)
        simplified_features.append(simplified)
    
    simplified_geojson = {
        'type': 'FeatureCollection',
        'features': simplified_features
    }
    
    # Санитизируем ID для имени файла
    safe_id = city_id.replace('/', '_').replace('\\', '_')
    filename = f"{safe_id}.geojson"
    filepath = output_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(simplified_geojson, f, ensure_ascii=False, indent=2)
    
    return filepath


def merge_all_city_zones(city_zones_dir, output_path):
    """
    Объединение всех файлов зон городов в один GeoJSON.
    
    Args:
        city_zones_dir: Path, директория с файлами городов
        output_path: Path, путь для сохранения объединённого файла
    
    Returns:
        dict с статистикой: {cities_count, total_features, zone_types}
    """
    if not city_zones_dir.exists():
        print("⚠️  Директория с зонами городов не найдена")
        return None
    
    # Собираем все GeoJSON файлы
    geojson_files = list(city_zones_dir.glob('*.geojson'))
    
    if not geojson_files:
        print("⚠️  Файлы с зонами не найдены")
        return None
    
    print()
    print("🔗 Объединяю все зоны в один файл...")
    print(f"   Найдено файлов: {len(geojson_files)}")
    
    # Используем dict для дедупликации по id
    unique_features = {}
    zone_types_total = {}
    cities_processed = 0
    duplicates_found = 0
    
    for geojson_file in geojson_files:
        try:
            with open(geojson_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            features = data.get('features', [])
            cities_processed += 1
            
            for feature in features:
                feature_id = feature.get('id')
                
                # Дедупликация: если зона уже есть, пропускаем
                if feature_id in unique_features:
                    duplicates_found += 1
                    continue
                
                unique_features[feature_id] = feature
                
                # Подсчёт типов зон
                zone_type = feature.get('properties', {}).get('type')
                if zone_type:
                    zone_types_total[zone_type] = zone_types_total.get(zone_type, 0) + 1
            
        except Exception as e:
            print(f"   ⚠️  Ошибка при чтении {geojson_file.name}: {e}")
            continue
    
    # Создаём объединённый GeoJSON из уникальных зон
    all_features = list(unique_features.values())
    merged_geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }
    
    # Сохраняем
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_geojson, f, ensure_ascii=False, indent=2)
    
    file_size_mb = output_path.stat().st_size / 1024 / 1024
    
    print(f"   ✅ Объединено городов: {cities_processed}")
    print(f"   ✅ Всего зон: {len(all_features)}")
    if duplicates_found > 0:
        print(f"   🔄 Дубликатов удалено: {duplicates_found}")
    print(f"   📊 Типы зон:")
    for zt, count in sorted(zone_types_total.items()):
        print(f"      {zt}: {count}")
    print(f"   💾 Размер файла: {file_size_mb:.1f} MB")
    print(f"   📁 Сохранено: {output_path}")
    
    return {
        'cities_count': cities_processed,
        'total_features': len(all_features),
        'zone_types': zone_types_total
    }


def parse_arguments():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Загрузка детальных зон для всех городов из cities.geojson',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 fetch_zones.py                     # Все города
  python3 fetch_zones.py --city "Сочи"       # Только указанный город
  python3 fetch_zones.py --continue_from 15  # Продолжить с города #15
        """
    )
    
    parser.add_argument('--city', type=str,
                       help='Название города из cities_list.csv (например: Сочи)')
    
    parser.add_argument('--continue_from', type=int, metavar='N',
                       help='Продолжить с города N')
    
    parser.add_argument('--zoom', type=float, default=16.7,
                       help='Уровень зума для детализации (по умолчанию: 16.7)')
    
    parser.add_argument('--delay', type=float, default=0.15,
                       help='Задержка между запросами в секундах (по умолчанию: 0.15)')
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    print("🚀 Загрузка детальных зон для всех городов")
    print("="*80)
    
    # Загрузка конфигурации
    headers = load_config()
    
    # Пути к файлам
    base_dir = Path(__file__).parent
    cities_geojson = base_dir / 'output' / 'cities.geojson'
    output_dir = base_dir / 'output' / 'city_zones'
    
    # Если указан --city, обрабатываем только этот город
    if args.city:
        city_zones = find_cities_by_name(args.city)
        
        if len(city_zones) > 1:
            print(f"🌍 Город '{args.city}' содержит {len(city_zones)} зон")
        
        print()
        
        # Обработка всех зон города
        total_zones = 0
        for idx, zone in enumerate(city_zones, 1):
            if len(city_zones) > 1:
                print(f"\n{'=' * 80}")
                print(f"📍 Зона {idx}/{len(city_zones)}: {zone['id']}")
                print(f"{'=' * 80}")
            
            bbox = zone['bbox']
            location = [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2]
            
            print(f"   📍 Bbox: {bbox}")
            print(f"   📍 Center: {location}")
            
            # Загрузка зон
            zones = fetch_city_zones(zone['id'], location, bbox, args.zoom, headers)
            
            if zones:
                # Сохранение
                output_path = save_city_zones(zone['id'], zones, output_dir)
                print(f"   ✅ Сохранено {len(zones)} зон → {output_path.name}")
                total_zones += len(zones)
            else:
                print(f"   ⚠️  Зоны не найдены")
            
            # Задержка между запросами
            if idx < len(city_zones):
                time.sleep(args.delay)
        
        print(f"\n{'=' * 80}")
        print(f"✅ Город '{args.city}' обработан!")
        print(f"   • Обработано зон города: {len(city_zones)}")
        print(f"   • Найдено зон API: {total_zones}")
        print(f"{'=' * 80}")
        
        return
    
    # Загрузка городов
    print(f"📥 Загружаю список городов из {cities_geojson.name}...")
    cities = load_city_polygons(cities_geojson)
    print(f"✅ Найдено городов: {len(cities)}")
    print()
    
    # Все города для обработки
    cities_to_process = cities
    
    # Продолжение с указанного города
    if args.continue_from:
        start_idx = args.continue_from - 1
        if start_idx >= len(cities):
            print(f"❌ Ошибка: город #{args.continue_from} не существует (всего {len(cities)})")
            sys.exit(1)
        
        cities_to_process = cities[start_idx:]
        print(f"🔄 Продолжение с города #{args.continue_from}")
        print()
    
    print(f"📊 Параметры:")
    print(f"   Городов для обработки: {len(cities_to_process)}")
    print(f"   Zoom: {args.zoom}")
    print(f"   Задержка между запросами: {args.delay}с")
    print()
    
    # Статистика
    total_cities = len(cities_to_process)
    successful = 0
    failed = 0
    empty = 0
    total_zones = 0
    
    start_time = time.time()
    
    print("="*80)
    print()
    
    # Обработка городов
    for idx, city in enumerate(cities_to_process, start=1):
        city_id = city['id']
        bbox = city['bbox']
        location = city['centroid']
        
        # Глобальный индекс (если используется --continue_from)
        global_idx = cities.index(city) + 1
        
        print(f"[{idx}/{total_cities}] Город #{global_idx}: {city_id}")
        print(f"   📍 Bbox: {bbox}")
        print(f"   📍 Center: {location}")
        
        # Загрузка зон
        zones = fetch_city_zones(city_id, location, bbox, args.zoom, headers)
        
        if zones is None:
            print(f"   ❌ Не удалось загрузить зоны")
            failed += 1
            
            # При HTTP 405 останавливаемся
            if failed > 0 and idx > 1:
                print()
                print("⚠️  Возможно истёк JWT токен. Остановка.")
                print(f"   Для продолжения обновите токен и используйте:")
                print(f"   python3 fetch_zones.py --continue_from {global_idx}")
                break
            
            time.sleep(args.delay)
            continue
        
        features_count = len(zones.get('features', []))
        
        if features_count == 0:
            print(f"   ⚠️  Зоны не найдены (0 полигонов)")
            empty += 1
        else:
            # Подсчёт типов зон (безопасный вариант)
            zone_types = {}
            for feature in zones.get('features', []):
                options = feature.get('properties', {}).get('options', [])
                zone_type = None
                for opt in options:
                    for action in opt.get('actions', []):
                        zone_type = action.get('zone_type')
                        if zone_type:
                            break
                    if zone_type:
                        break
                if zone_type:
                    zone_types[zone_type] = zone_types.get(zone_type, 0) + 1
            
            zone_summary = ', '.join([f"{zt}: {cnt}" for zt, cnt in zone_types.items()]) if zone_types else 'границы'
            
            print(f"   ✅ Загружено зон: {features_count} ({zone_summary})")
            
            # Сохранение
            filepath = save_city_zones(city_id, zones, output_dir)
            print(f"   💾 Сохранено: {filepath.name}")
            
            successful += 1
            total_zones += features_count
        
        print()
        
        # Задержка между запросами
        if idx < total_cities:
            time.sleep(args.delay)
    
    # Итоговая статистика
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    print("="*80)
    print()
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Обработано городов: {idx}/{total_cities}")
    print(f"   ✅ Успешно: {successful}")
    print(f"   ⚠️  Пустые: {empty}")
    print(f"   ❌ Ошибки: {failed}")
    print(f"   📍 Всего зон загружено: {total_zones}")
    print(f"   ⏱️  Время выполнения: {minutes}м {seconds}с")
    print()
    print(f"📁 Отдельные файлы сохранены в: {output_dir}")
    
    # Объединение всех файлов в один
    if successful > 0:
        merged_file = base_dir / 'output' / 'zones.geojson'
        merge_all_city_zones(output_dir, merged_file)
    
    print()
    print("="*80)
    print("✅ Готово!")


if __name__ == "__main__":
    main()
