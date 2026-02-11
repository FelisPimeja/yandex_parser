#!/usr/bin/env python3
"""
Скрипт для загрузки зон доступности самокатов Yandex Go по всему миру.

Использует глобальную сетку 3×3° с 50% перекрытием из файла grid_3x3.geojson
для покрытия всей планеты (28,441 квадрат).

API возвращает "scooters_polygon" - зоны, где можно брать и оставлять самокаты.
Это НЕ административные границы городов, а рабочие зоны сервиса.

Важно:
- Требуется поле "night_mode": False в запросе
- API исключает объекты на краях bbox (нужно 30-70% от высоты)
- JWT токен (X-Yandex-Jws) действует 1 час
- Один город может иметь несколько полигонов (например, Москва = 17 зон)

Режимы работы:
По умолчанию:     Обновление известных городов (быстро, ~2 минуты)
--search_new:     Поиск новых городов (долго, ~20 часов)
--continue_from:  Продолжить поиск с указанного квадрата
"""

import json
import sys
import time
import re
import argparse
from pathlib import Path
import requests
from datetime import datetime

BASE_URL = "https://tc.mobile.yandex.net"

def load_config():
    config_path = Path(__file__).parent / 'config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config.get('yandex_headers')

def fetch_cities_in_region(bbox, headers, verbose=False):
    """
    Запрашивает зоны самокатов для указанного bbox.
    
    Args:
        bbox: [min_lon, min_lat, max_lon, max_lat]
        headers: заголовки запроса с JWT токеном
        verbose: выводить ли детальную информацию
    
    Returns:
        (list of features, error_message or None)
    """
    url = f"{BASE_URL}/4.0/layers/v1/polygons"
    params = {
        "mobcf": "russia%25go_ru_by_geo_hosts_2%25default",
        "mobpr": "go_ru_by_geo_hosts_2_TAXI_V4_0"
    }
    
    # Вычисляем центр bbox для поля location
    center_lon = (bbox[0] + bbox[2]) / 2
    center_lat = (bbox[1] + bbox[3]) / 2
    
    # КРИТИЧНО: "night_mode": False обязательно для работы!
    data = {
        "known_versions": {},
        "state": {
            "multiclass_options": {"selected": False},
            "bbox": bbox,
            "zoom": 8.0,
            "scooters": {"autoselect": False},
            "known_orders_info": [],
            "screen": "discovery",
            "night_mode": False,  # ⚠️ ОБЯЗАТЕЛЬНО!
            "known_orders": [],
            "location": [center_lon, center_lat],
            "mode": "scooters"
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, params=params, timeout=15)
        
        # Проверка на HTTP 405 = истёк JWT токен
        if response.status_code == 405:
            return [], "❌ HTTP 405: JWT токен истёк! Обновите X-Yandex-Jws в config.json"
        
        response.raise_for_status()
        result = response.json()
        
        # Проверяем структуру ответа
        if verbose and 'features' not in result:
            return [], f"⚠️  Нет 'features' в ответе. Ключи: {list(result.keys())}"
        
        # Фильтруем только scooters_polygon
        scooters_polygons = []
        all_features = result.get('features', [])
        
        for f in all_features:
            props = f.get('properties', {})
            if props.get('type') == 'scooters_polygon':
                scooters_polygons.append(f)
        
        if verbose and all_features:
            print(f"\n      DEBUG: Получено {len(all_features)} features, из них {len(scooters_polygons)} scooters_polygon")
        
        return scooters_polygons, None
        
    except requests.exceptions.HTTPError as e:
        return [], f"❌ HTTP {e.response.status_code}: {e.response.text[:100]}"
    except requests.exceptions.Timeout:
        return [], "❌ Timeout (15s)"
    except requests.exceptions.RequestException as e:
        return [], f"❌ Request error: {str(e)[:100]}"
    except Exception as e:
        return [], f"❌ Error: {str(e)[:100]}"

def simplify_polygon_feature(feature):
    """Упрощает данные полигона, оставляя только полезные поля."""
    props = feature.get('properties', {})
    
    return {
        'type': 'Feature',
        'id': feature.get('id'),
        'geometry': feature.get('geometry'),
        'properties': {
            'type': props.get('type'),
            'version': props.get('version'),
            'zooms': props.get('display_settings', {}).get('zooms', [])
        }
    }

def check_token_expiry(headers):
    """Проверяет время до истечения JWT токена."""
    try:
        import base64
        jwt_token = headers.get('X-Yandex-Jws', '')
        if not jwt_token:
            return None
        
        # Декодируем payload (вторая часть JWT)
        parts = jwt_token.split('.')
        if len(parts) < 2:
            return None
        
        payload = parts[1]
        # Добавляем padding если нужно
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding
        
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        
        expires_at_ms = data.get('expires_at_ms', 0)
        expires_at = expires_at_ms / 1000  # в секунды
        now = time.time()
        remaining = expires_at - now
        
        return remaining
    except:
        return None

def save_results(all_polygons, stage_name, timestamp):
    """Сохраняет результаты сканирования."""
    Path('output/tmp').mkdir(parents=True, exist_ok=True)
    
    # Сохраняем сырые данные
    raw_geojson = {'type': 'FeatureCollection', 'features': list(all_polygons.values())}
    raw_file = f'output/tmp/scooter_zones_{stage_name}_{timestamp}.json'
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(raw_geojson, f, indent=2, ensure_ascii=False)
    
    # Сохраняем упрощённую версию
    simplified_features = [simplify_polygon_feature(poly) for poly in all_polygons.values()]
    simplified_geojson = {'type': 'FeatureCollection', 'features': simplified_features}
    with open('output/cities.geojson', 'w', encoding='utf-8') as f:
        json.dump(simplified_geojson, f, indent=2, ensure_ascii=False)
    
    # Сохраняем список уникальных ID
    id_list_file = f'output/tmp/polygon_ids_{stage_name}_{timestamp}.txt'
    with open(id_list_file, 'w', encoding='utf-8') as f:
        for polygon_id in sorted(all_polygons.keys()):
            f.write(f'{polygon_id}\n')
    
    return raw_file, id_list_file

def log_progress(log_file, square_id, found_count, message=""):
    """Добавляет запись в лог-файл."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] Square #{square_id}: {found_count} polygons found {message}\n")

def update_grid_from_log(grid_data, log_file):
    """
    Обновляет сетку из лога, добавляя has_city=True для квадратов с найденными полигонами.
    Возвращает количество обновлённых квадратов.
    """
    if not log_file.exists():
        return 0
    
    print(f"\n📝 Обнаружен лог: {log_file}")
    print(f"   Проверяю наличие новых данных...")
    
    # Парсим лог
    squares_with_cities = {}  # square_id -> polygon_count
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # Ищем строки вида: [timestamp] Square #ID: N polygons found
            match = re.search(r'Square #(\d+):\s*(\d+)\s+polygons?\s+found', line)
            if match:
                square_id = int(match.group(1))
                polygon_count = int(match.group(2))
                
                # Сохраняем только квадраты с городами
                if polygon_count > 0:
                    # Берём максимум, если квадрат встречается несколько раз
                    if square_id in squares_with_cities:
                        squares_with_cities[square_id] = max(squares_with_cities[square_id], polygon_count)
                    else:
                        squares_with_cities[square_id] = polygon_count
    
    if not squares_with_cities:
        print(f"   ℹ️  В логе нет данных о найденных городах")
        return 0
    
    print(f"   ✓ В логе найдено {len(squares_with_cities)} квадратов с городами")
    
    # Обновляем атрибуты в сетке
    updated_count = 0
    new_cities = 0
    
    for feature in grid_data['features']:
        square_id = feature['properties'].get('id')
        
        if square_id in squares_with_cities:
            was_known = feature['properties'].get('has_city') == True
            
            # Обновляем атрибуты
            feature['properties']['has_city'] = True
            feature['properties']['tested'] = True
            feature['properties']['polygon_count'] = squares_with_cities[square_id]
            
            if not was_known:
                new_cities += 1
            
            updated_count += 1
    
    if new_cities > 0:
        print(f"   🆕 Добавлено {new_cities} новых квадратов с городами")
        print(f"   💾 Сохраняю обновлённую сетку...")
        
        # Сохраняем обновлённую сетку
        grid_path = Path(__file__).parent / 'grid_3x3.geojson'
        with open(grid_path, 'w', encoding='utf-8') as f:
            json.dump(grid_data, f, ensure_ascii=False, indent=2)
        
        print(f"   ✅ Сетка обновлена")
    else:
        print(f"   ℹ️  Все квадраты из лога уже были в сетке")
    
    return new_cities

def parse_arguments():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Загрузка зон доступности самокатов Yandex Go',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 fetch_cities.py                    # Обновить известные города (~2 мин)
  python3 fetch_cities.py --search_new       # Поиск новых городов (~2 часа)
  python3 fetch_cities.py --search_new --continue_from 21156  # Продолжить поиск начиная с квадрата #21156
        """
    )
    
    parser.add_argument(
        '--continue_from',
        type=int,
        metavar='SQUARE_ID',
        help='Продолжить сканирование с указанного квадрата (по ID). Автоматически включает --search_new'
    )
    
    parser.add_argument(
        '--search_new',
        action='store_true',
        help='Искать новые города в неизвестных квадратах (долгое сканирование). По умолчанию обрабатываются только известные города'
    )
    
    return parser.parse_args()

def main():
    # Парсим аргументы
    args = parse_arguments()
    print("🚀 Загрузка зон самокатов Yandex Go по всему миру")
    print("="*80)
    
    headers = load_config()
    
    # Проверяем токен
    remaining = check_token_expiry(headers)
    if remaining:
        remaining_min = int(remaining / 60)
        print(f"🔐 JWT токен действителен ещё {remaining_min} минут")
        if remaining < 600:  # меньше 10 минут
            print("   ⚠️  ВНИМАНИЕ: Токен скоро истечёт! Обновите config.json")
    else:
        print("⚠️  Не удалось проверить срок действия токена")
    
    # Загружаем глобальную сетку 3×3° с 50% перекрытием
    print("\n📍 Загружаю глобальную сетку покрытия...")
    
    grid_path = Path(__file__).parent / 'grid_3x3.geojson'
    with open(grid_path, 'r', encoding='utf-8') as f:
        grid_data = json.load(f)
    
    total_squares = len(grid_data['features'])
    print(f"   ✓ Загружено {total_squares:,} квадратов из grid_3x3.geojson")
    
    # Разделяем квадраты на известные (has_city=true) и неизвестные
    known_squares = [f for f in grid_data['features'] if f['properties'].get('has_city') == True]
    unknown_squares = [f for f in grid_data['features'] if f['properties'].get('has_city') != True]
    
    print(f"   ℹ️  Квадратов с известными городами: {len(known_squares):,}")
    print(f"   ℹ️  Квадратов для поиска: {len(unknown_squares):,}")
    
    # Расчёт времени
    estimated_seconds = total_squares * 0.15  # 0.15 сек на запрос
    estimated_minutes = estimated_seconds / 60
    estimated_hours = estimated_minutes / 60
    
    print(f"\n⏱️  Ожидаемое время сканирования:")
    print(f"   • Этап 1 (известные): {len(known_squares) * 0.15 / 6:.1f} минут")
    print(f"   • Этап 2 (поиск новых): {len(unknown_squares) * 0.15 / 60:.0f} минут")
    print(f"   • Всего с паузами: ~{estimated_hours * 1.1:.1f} часа")
    
    # Инициализация
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path('output/tmp/fetch_cities_log.txt')
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Начало лога
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"=== Fetch Cities Log {timestamp} ===\n")
        f.write(f"Total squares: {total_squares:,}\n")
        f.write(f"Known squares: {len(known_squares):,}\n")
        f.write(f"Unknown squares: {len(unknown_squares):,}\n\n")
    
    print(f"\n� Лог сохраняется в: {log_file}")
    print(f"\n{'='*80}\n")
def main():
    # Парсим аргументы
    args = parse_arguments()
    
    # Если указан --continue_from, автоматически включаем --search_new
    if args.continue_from:
        args.search_new = True
    
    print("🚀 Загрузка зон самокатов Yandex Go по всему миру")
    print("="*80)
    
    # Показываем активный режим
    if args.search_new:
        if args.continue_from:
            print(f"\n� Режим: Поиск новых городов (продолжение с #{args.continue_from})")
        else:
            print(f"\n🔍 Режим: Поиск новых городов (полное сканирование)")
    else:
        print(f"\n📥 Режим: Обновление известных городов (быстрое)")
        print(f"   💡 Для поиска новых городов запустите скрипт с флагом --search_new")
    
    headers = load_config()
    
    # Проверяем токен
    remaining = check_token_expiry(headers)
    if remaining:
        remaining_min = int(remaining / 60)
        print(f"\n🔐 JWT токен действителен ещё {remaining_min} минут")
        if remaining < 600:  # меньше 10 минут
            print("   ⚠️  ВНИМАНИЕ: Токен скоро истечёт! Обновите config.json")
    else:
        print("\n⚠️  Не удалось проверить срок действия токена")
    
    # Загружаем глобальную сетку 3×3° с 50% перекрытием
    print("\n📍 Загружаю глобальную сетку покрытия...")
    
    grid_path = Path(__file__).parent / 'grid_3x3.geojson'
    with open(grid_path, 'r', encoding='utf-8') as f:
        grid_data = json.load(f)
    
    total_squares = len(grid_data['features'])
    print(f"   ✓ Загружено {total_squares:,} квадратов из grid_3x3.geojson")
    
    # Инициализация лог-файла
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path('output/tmp/fetch_cities_log.txt')
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Проверяем и обновляем сетку из существующего лога
    new_cities_from_log = update_grid_from_log(grid_data, log_file)
    
    if new_cities_from_log > 0:
        # Перезагружаем сетку после обновления
        with open(grid_path, 'r', encoding='utf-8') as f:
            grid_data = json.load(f)
    
    # Разделяем квадраты на известные (has_city=true) и неизвестные
    known_squares = [f for f in grid_data['features'] if f['properties'].get('has_city') == True]
    unknown_squares = [f for f in grid_data['features'] if f['properties'].get('has_city') != True]
    
    # Применяем фильтр --continue_from
    if args.continue_from:
        # Фильтруем unknown_squares, оставляя только с ID >= continue_from
        unknown_squares = [f for f in unknown_squares if f['properties'].get('id', 0) >= args.continue_from]
        print(f"\n🔄 После фильтрации (>= #{args.continue_from}):")
        print(f"   • Квадратов для сканирования: {len(unknown_squares):,}")
        
        if not unknown_squares:
            print(f"\n⚠️  Нет квадратов с ID >= {args.continue_from}")
            print(f"   Возможно, все уже обработаны или неверный ID")
            return
    
    print(f"\n📊 Статистика сетки:")
    print(f"   ℹ️  Квадратов с известными городами: {len(known_squares):,}")
    
    if args.search_new:
        print(f"   ℹ️  Квадратов для поиска: {len(unknown_squares):,}")
    else:
        print(f"   ℹ️  Квадратов для поиска: {len(unknown_squares):,} (пропущены, используйте --search_new)")
    
    # Расчёт времени
    stage1_time = len(known_squares) * 0.15 / 60 if not args.continue_from else 0
    stage2_time = len(unknown_squares) * 0.15 / 60 if args.search_new else 0
    estimated_minutes = stage1_time + stage2_time
    estimated_hours = estimated_minutes / 60
    
    print(f"\n⏱️  Ожидаемое время сканирования:")
    if not args.continue_from:
        print(f"   • Этап 1 (известные): {stage1_time:.1f} минут")
    if args.search_new:
        print(f"   • Этап 2 (поиск новых): {stage2_time:.0f} минут")
        print(f"   • Всего с паузами: ~{estimated_hours * 1.1:.1f} часа")
    else:
        print(f"   • Всего: ~{stage1_time:.1f} минут")
    
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
    
    print(f"\n📝 Лог сохраняется в: {log_file}")
    print(f"\n{'='*80}\n")
    
    all_polygons = {}  # polygon_id -> feature
    errors = []
    start_time = time.time()
    
    # ========================================================================
    # ЭТАП 1: Скачиваем известные города
    # ========================================================================
    
    # Пропускаем этап 1 если используется --continue_from
    if known_squares and not args.continue_from:
        print("📥 ЭТАП 1: Скачиваем известные города...")
        print(f"   Квадратов для обработки: {len(known_squares):,}\n")
        
        stage1_start = time.time()
        squares_with_polygons = 0
        
        # Начальный прогресс-бар
        bar = '░' * 50
        print(f'   [{bar}] 0.0% (0/{len(known_squares):,})', end='', flush=True)
        
        for idx, feature in enumerate(known_squares, 1):
            props = feature['properties']
            bbox = [props['left'], props['bottom'], props['right'], props['top']]
            square_id = props.get('id', '?')
            
            # Первые 2 запроса - с verbose режимом для диагностики
            verbose = idx <= 2
            
            if verbose:
                print(f'\r{" " * 150}\r   🔍 Запрос #{idx}: Square #{square_id}...', end='', flush=True)
            
            polygons, error = fetch_cities_in_region(bbox, headers, verbose=verbose)
            
            new_polygon_msg = None
            
            # Обработка ошибок
            if error:
                errors.append({'stage': 1, 'square_id': square_id, 'bbox': bbox, 'error': error, 'idx': idx})
                log_progress(log_file, square_id, 0, f"ERROR: {error}")
                
                # Показываем первые 2 ошибки
                if len(errors) <= 2:
                    new_polygon_msg = f"   [Этап 1] Square #{square_id:<10} {error}"
                
                # КРИТИЧНО: HTTP 405 = истёк токен, останавливаем
                if "HTTP 405" in error:
                    print(f'\n\n❌ ОСТАНОВЛЕНО: JWT токен истёк после {idx} запросов этапа 1!')
                    print(f'   Обновите X-Yandex-Jws в config.json и запустите снова.')
                    print(f'   Уже найдено {len(all_polygons)} уникальных полигонов.')
                    break
            
            # Обработка найденных полигонов
            found_count = 0
            if polygons:
                new_polygons = []
                for polygon in polygons:
                    polygon_id = polygon.get('id')
                    if polygon_id and polygon_id not in all_polygons:
                        all_polygons[polygon_id] = polygon
                        new_polygons.append(polygon_id)
                
                found_count = len(polygons)
                if new_polygons or polygons:
                    squares_with_polygons += 1
                    # Показываем сообщение если нашли новые полигоны
                    if new_polygons:
                        new_polygon_msg = f"   [Этап 1] Square #{square_id:<10} ✅ +{len(new_polygons)} новых (всего: {len(all_polygons)})"
            
            # Логируем каждые 50 запросов или при наличии полигонов
            if idx % 50 == 0 or found_count > 0:
                log_progress(log_file, square_id, found_count)
            
            # Прогресс-бар
            progress = idx / len(known_squares)
            bar_length = 50
            filled = int(bar_length * progress)
            bar = '█' * filled + '░' * (bar_length - filled)
            
            elapsed = time.time() - stage1_start
            if idx > 0:
                avg_time = elapsed / idx
                remaining_requests = len(known_squares) - idx
                eta_seconds = remaining_requests * avg_time
                eta_minutes = eta_seconds / 60
                eta_str = f"ETA: {eta_minutes:.0f}m" if eta_minutes > 1 else f"ETA: {eta_seconds:.0f}s"
            else:
                eta_str = "ETA: calculating..."
            
            progress_line = f'   [{bar}] {progress*100:.1f}% ({idx:,}/{len(known_squares):,}) | {len(all_polygons)} полигонов | {eta_str}'
            
            # Выводим сообщение о новых полигонах (если есть) и прогресс-бар
            if new_polygon_msg:
                print(f'\r{" " * 150}\r{new_polygon_msg}')
            print(f'\r{progress_line}', end='', flush=True)
            
            time.sleep(0.15)  # Задержка между запросами
            
            # Пауза каждые 50 запросов
            if idx % 50 == 0:
                print(f'\r{" " * 150}\r   ⏸️  Пауза 10 сек после {idx:,} запросов...', end='', flush=True)
                time.sleep(10)
                print(f'\r{" " * 150}\r', end='', flush=True)
            
            # Проверка токена каждые 500 запросов
            if idx % 500 == 0:
                remaining = check_token_expiry(headers)
                if remaining and remaining < 300:  # меньше 5 минут
                    print(f'\r{" " * 150}\r   ⚠️  ВНИМАНИЕ: Токен истекает через {int(remaining/60)} минут!')
        
        print()  # Новая строка после прогресс-бара
        
        stage1_time = time.time() - stage1_start
        print(f"\n✅ Этап 1 завершён за {stage1_time/60:.1f} минут")
        print(f"   • Обработано квадратов: {idx:,}/{len(known_squares):,}")
        print(f"   • Квадратов с полигонами: {squares_with_polygons:,}")
        print(f"   • Найдено уникальных полигонов: {len(all_polygons):,}")
        
        # Сохраняем результаты этапа 1
        if all_polygons:
            print(f"\n💾 Сохраняю известные города...")
            raw_file, id_file = save_results(all_polygons, 'stage1_known', timestamp)
            print(f"   ✓ Сохранено {len(all_polygons):,} полигонов")
            print(f"   • output/cities.geojson")
            print(f"   • {raw_file}")
            print(f"   • {id_file}")
        
        # Запись в лог
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n=== STAGE 1 COMPLETE ===\n")
            f.write(f"Time: {stage1_time/60:.1f} minutes\n")
            f.write(f"Squares processed: {idx}/{len(known_squares)}\n")
            f.write(f"Unique polygons: {len(all_polygons)}\n\n")
    
    # ========================================================================
    # ЭТАП 2: Поиск новых городов
    # ========================================================================
    
    # Выполняем этап 2 только если указан --search_new
    if unknown_squares and args.search_new:
        print(f"\n{'='*80}\n")
        print("🔍 ЭТАП 2: Поиск новых городов...")
        
        if args.continue_from:
            print(f"   🔄 Продолжение с квадрата #{args.continue_from}")
        
        print(f"   Квадратов для обработки: {len(unknown_squares):,}\n")
        
        stage2_start = time.time()
        squares_with_polygons_stage2 = 0
        polygons_before_stage2 = len(all_polygons)
        
        # Начальный прогресс-бар
        bar = '░' * 50
        print(f'   [{bar}] 0.0% (0/{len(unknown_squares):,})', end='', flush=True)
        
        for idx, feature in enumerate(unknown_squares, 1):
            props = feature['properties']
            bbox = [props['left'], props['bottom'], props['right'], props['top']]
            square_id = props.get('id', '?')
            
            # Первые 2 запроса - с verbose режимом для диагностики
            verbose = idx <= 2 and len(known_squares) == 0  # Только если этап 1 был пропущен
            
            polygons, error = fetch_cities_in_region(bbox, headers, verbose=verbose)
            
            new_polygon_msg = None
            
            # Обработка ошибок
            if error:
                errors.append({'stage': 2, 'square_id': square_id, 'bbox': bbox, 'error': error, 'idx': idx})
                log_progress(log_file, square_id, 0, f"ERROR: {error}")
                
                # КРИТИЧНО: HTTP 405 = истёк токен, останавливаем
                if "HTTP 405" in error:
                    print(f'\n\n❌ ОСТАНОВЛЕНО: JWT токен истёк после {idx} запросов этапа 2!')
                    print(f'   Обновите X-Yandex-Jws в config.json и запустите снова.')
                    print(f'   Уже найдено {len(all_polygons)} уникальных полигонов.')
                    break
            
            # Обработка найденных полигонов
            found_count = 0
            if polygons:
                new_polygons = []
                for polygon in polygons:
                    polygon_id = polygon.get('id')
                    if polygon_id and polygon_id not in all_polygons:
                        all_polygons[polygon_id] = polygon
                        new_polygons.append(polygon_id)
                
                found_count = len(polygons)
                if new_polygons or polygons:
                    squares_with_polygons_stage2 += 1
                    # Показываем сообщение если нашли НОВЫЕ полигоны
                    if new_polygons:
                        new_polygon_msg = f"   [Этап 2] Square #{square_id:<10} 🆕 +{len(new_polygons)} НОВЫХ! (всего: {len(all_polygons)})"
            
            # Логируем каждые 50 запросов или при наличии полигонов
            if idx % 50 == 0 or found_count > 0:
                log_progress(log_file, square_id, found_count, "NEW!" if found_count > 0 else "")
            
            # Прогресс-бар
            progress = idx / len(unknown_squares)
            bar_length = 50
            filled = int(bar_length * progress)
            bar = '█' * filled + '░' * (bar_length - filled)
            
            elapsed = time.time() - stage2_start
            if idx > 0:
                avg_time = elapsed / idx
                remaining_requests = len(unknown_squares) - idx
                eta_seconds = remaining_requests * avg_time
                eta_minutes = eta_seconds / 60
                eta_str = f"ETA: {eta_minutes:.0f}m" if eta_minutes > 1 else f"ETA: {eta_seconds:.0f}s"
            else:
                eta_str = "ETA: calculating..."
            
            new_count = len(all_polygons) - polygons_before_stage2
            progress_line = f'   [{bar}] {progress*100:.1f}% ({idx:,}/{len(unknown_squares):,}) | Новых: {new_count} | Всего: {len(all_polygons)} | {eta_str}'
            
            # Выводим сообщение о новых полигонах (если есть) и прогресс-бар
            if new_polygon_msg:
                print(f'\r{" " * 150}\r{new_polygon_msg}')
            print(f'\r{progress_line}', end='', flush=True)
            
            time.sleep(0.15)  # Задержка между запросами
            
            # Пауза каждые 50 запросов
            if idx % 50 == 0:
                print(f'\r{" " * 150}\r   ⏸️  Пауза 10 сек после {idx:,} запросов...', end='', flush=True)
                time.sleep(10)
                print(f'\r{" " * 150}\r', end='', flush=True)
            
            # Проверка токена каждые 500 запросов
            if idx % 500 == 0:
                remaining = check_token_expiry(headers)
                if remaining and remaining < 300:  # меньше 5 минут
                    print(f'\r{" " * 150}\r   ⚠️  ВНИМАНИЕ: Токен истекает через {int(remaining/60)} минут!')
        
        print()  # Новая строка после прогресс-бара
        
        stage2_time = time.time() - stage2_start
        new_polygons_found = len(all_polygons) - polygons_before_stage2
        
        print(f"\n✅ Этап 2 завершён за {stage2_time/60:.1f} минут")
        print(f"   • Обработано квадратов: {idx:,}/{len(unknown_squares):,}")
        print(f"   • Квадратов с полигонами: {squares_with_polygons_stage2:,}")
        print(f"   • Найдено НОВЫХ полигонов: {new_polygons_found:,}")
        print(f"   • Всего уникальных полигонов: {len(all_polygons):,}")
        
        # Сохраняем результаты этапа 2 (если были найдены новые)
        if new_polygons_found > 0:
            print(f"\n💾 Сохраняю обновлённые результаты с новыми городами...")
            raw_file, id_file = save_results(all_polygons, 'stage2_complete', timestamp)
            print(f"   ✓ Сохранено {len(all_polygons):,} полигонов (+{new_polygons_found} новых)")
            print(f"   • output/cities.geojson")
            print(f"   • {raw_file}")
            print(f"   • {id_file}")
        else:
            print(f"\n   ℹ️  Новых полигонов не найдено, файлы не обновлялись")
        
        # Запись в лог
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n=== STAGE 2 COMPLETE ===\n")
            f.write(f"Time: {stage2_time/60:.1f} minutes\n")
            f.write(f"Squares processed: {idx}/{len(unknown_squares)}\n")
            f.write(f"New polygons: {new_polygons_found}\n")
            f.write(f"Total polygons: {len(all_polygons)}\n\n")
    
    # ========================================================================
    # ИТОГОВАЯ СТАТИСТИКА
    # ========================================================================
    
    elapsed_total = time.time() - start_time
    elapsed_minutes = elapsed_total / 60
    
    print(f"\n{'='*80}")
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:\n")
    print(f"   • Всего квадратов: {total_squares:,}")
    print(f"   • Известных квадратов: {len(known_squares):,}")
    print(f"   • Неизвестных квадратов: {len([f for f in grid_data['features'] if f['properties'].get('has_city') != True]):,}")
    print(f"   • Всего найдено уникальных полигонов: {len(all_polygons):,}")
    print(f"   • Время выполнения: {elapsed_minutes:.1f} минут ({elapsed_total/3600:.2f} часа)")
    
    # Информация о режиме работы
    if args.continue_from:
        print(f"\n🔄 Режим продолжения:")
        print(f"   • Начато с квадрата: #{args.continue_from}")
    if not args.search_new:
        print(f"\n📥 Режим: Только известные города")
        print(f"   • Этап 2 (поиск новых) был пропущен")
        print(f"   • Для поиска новых используйте: --search_new")
    
    # Отчёт об ошибках
    if errors:
        print(f"\n⚠️  Ошибок при запросах: {len(errors)}")
        errors_stage1 = [e for e in errors if e.get('stage') == 1]
        errors_stage2 = [e for e in errors if e.get('stage') == 2]
        if errors_stage1:
            print(f"   • Этап 1: {len(errors_stage1)} ошибок")
        if errors_stage2:
            print(f"   • Этап 2: {len(errors_stage2)} ошибок")
        
        print("\n   Первые 5 ошибок:")
        for err in errors[:5]:
            stage_label = f"Этап {err.get('stage', '?')}"
            print(f"   • [{stage_label}] Square {err['square_id']}: {err['error']}")
        if len(errors) > 5:
            print(f"   ... и ещё {len(errors) - 5} ошибок")
    
    # Топ-10 полигонов для примера
    if all_polygons:
        print(f"\n🏙️  Примеры найденных полигонов (первые 10):")
        for i, polygon_id in enumerate(list(all_polygons.keys())[:10], 1):
            print(f"   {i:2d}. {polygon_id}")
        if len(all_polygons) > 10:
            print(f"   ... и ещё {len(all_polygons) - 10} полигонов")
    
    # Финальная запись в лог
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n=== FINAL SUMMARY ===\n")
        f.write(f"Total time: {elapsed_minutes:.1f} minutes\n")
        f.write(f"Total polygons: {len(all_polygons)}\n")
        f.write(f"Total errors: {len(errors)}\n")
        f.write(f"Completion: SUCCESS\n")
    
    print(f"\n💾 СОХРАНЁННЫЕ ФАЙЛЫ:")
    print(f"   • output/cities.geojson (для визуализации)")
    print(f"   • output/tmp/scooter_zones_*.json (полные данные)")
    print(f"   • output/tmp/polygon_ids_*.txt (список ID)")
    print(f"   • {log_file} (лог выполнения)")
    
if __name__ == "__main__":
    main()
