#!/usr/bin/env python3
"""
Скрипт для конвертации зон ограничений (restriction zones) в GeoJSON
API возвращает их вместе с rent zones, но это разные сущности:
- rent_zones - зоны аренды (где можно брать/оставлять транспорт)
- restriction_zones - зоны ограничений (lowSpeedZones, restrictedZones, notAllowedZones)

Обрабатывает три типа restriction zones:
- lowSpeedZones - зоны с ограничением скорости
- restrictedZones - зоны с запретом парковки
- notAllowedZones - зоны с запретом поездок
"""
import json
import sys
from pathlib import Path


def convert_zone_to_feature(zone: dict, zone_type: str) -> dict:
    """Конвертирует одну зону в GeoJSON Feature"""
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
        "type": zone_type
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
            "coordinates": [coordinates]  # GeoJSON Polygon требует массив массивов
        }
    }
    
    return feature


def convert_restriction_zones_to_geojson(data: dict) -> dict:
    """Конвертирует зоны ограничений из ответа API в GeoJSON"""
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Обрабатываем зоны с ограничением скорости
    for zone in data.get('data', {}).get('lowSpeedZones', []):
        feature = convert_zone_to_feature(zone, 'lowSpeedZone')
        if feature:
            geojson['features'].append(feature)
    
    # Обрабатываем зоны с запретом парковки
    for zone in data.get('data', {}).get('restrictedZones', []):
        feature = convert_zone_to_feature(zone, 'restrictedZone')
        if feature:
            geojson['features'].append(feature)
    
    # Обрабатываем зоны с запретом поездок
    for zone in data.get('data', {}).get('notAllowedZones', []):
        feature = convert_zone_to_feature(zone, 'notAllowedZone')
        if feature:
            geojson['features'].append(feature)
    
    return geojson


def main():
    """Основная функция"""
    # Проверяем аргументы командной строки
    if len(sys.argv) < 2:
        print("❌ Использование: python3 convert_restriction_zones_to_geojson.py <input_file.json> [output_file.geojson]")
        print("\nПример:")
        print("  python3 convert_restriction_zones_to_geojson.py sochi_restriction_zones.json")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    
    # Если не указан выходной файл, используем имя входного с расширением .geojson
    if len(sys.argv) >= 3:
        output_file = Path(sys.argv[2])
    else:
        output_file = input_file.with_suffix('.geojson')
    
    # Проверяем существование входного файла
    if not input_file.exists():
        print(f"❌ Файл не найден: {input_file}")
        sys.exit(1)
    
    print(f"📖 Чтение файла: {input_file}")
    
    # Читаем входной JSON
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        sys.exit(1)
    
    print(f"🔄 Конвертация в GeoJSON...")
    
    # Конвертируем в GeoJSON
    geojson = convert_restriction_zones_to_geojson(data)
    
    # Подсчитываем статистику
    stats = {
        'lowSpeedZone': 0,
        'restrictedZone': 0,
        'notAllowedZone': 0
    }
    
    for feature in geojson['features']:
        zone_type = feature['properties']['type']
        stats[zone_type] = stats.get(zone_type, 0) + 1
    
    # Сохраняем результат
    print(f"💾 Сохранение в: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    # Выводим статистику
    print(f"\n✅ Готово!")
    print(f"\n📊 Статистика:")
    print(f"  • Зоны с ограничением скорости: {stats['lowSpeedZone']}")
    print(f"  • Зоны с запретом парковки: {stats['restrictedZone']}")
    print(f"  • Зоны с запретом поездок: {stats['notAllowedZone']}")
    print(f"  • Всего зон: {len(geojson['features'])}")
    print(f"\n📁 Файл: {output_file}")


if __name__ == '__main__':
    main()
