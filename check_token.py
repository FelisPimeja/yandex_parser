#!/usr/bin/env python3
"""
Утилита для проверки срока действия JWT токена X-Yandex-Jws.

Декодирует JWT и показывает:
- Время создания токена
- Время истечения токена
- Оставшееся время до истечения
- UUID устройства
- IP адрес
"""

import json
import base64
from datetime import datetime, timedelta
from pathlib import Path


def decode_jwt_payload(jwt_token):
    """Декодирует payload JWT токена."""
    try:
        # JWT состоит из 3 частей: header.payload.signature
        parts = jwt_token.split('.')
        if len(parts) != 3:
            return None, "Неверный формат JWT (должно быть 3 части)"
        
        # Декодируем payload (вторая часть)
        payload = parts[1]
        
        # Добавляем padding если нужно
        padding = len(payload) % 4
        if padding:
            payload += '=' * (4 - padding)
        
        # Декодируем base64url
        decoded_bytes = base64.urlsafe_b64decode(payload)
        payload_data = json.loads(decoded_bytes.decode('utf-8'))
        
        return payload_data, None
        
    except Exception as e:
        return None, f"Ошибка декодирования: {e}"


def format_timedelta(td):
    """Форматирует timedelta в человекочитаемый вид."""
    total_seconds = int(td.total_seconds())
    
    if total_seconds < 0:
        return f"истёк {format_timedelta(-td)} назад"
    
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0:
        parts.append(f"{minutes}м")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}с")
    
    return " ".join(parts)


def check_token():
    """Проверяет JWT токен из config.json."""
    config_path = Path(__file__).parent / 'config.json'
    
    if not config_path.exists():
        print("❌ Файл config.json не найден!")
        return
    
    # Загружаем config
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    jwt_token = config.get('yandex_headers', {}).get('X-Yandex-Jws')
    
    if not jwt_token:
        print("❌ JWT токен (X-Yandex-Jws) не найден в config.json!")
        return
    
    print("🔍 Проверка JWT токена X-Yandex-Jws")
    print("=" * 70)
    print()
    
    # Декодируем payload
    payload, error = decode_jwt_payload(jwt_token)
    
    if error:
        print(f"❌ {error}")
        return
    
    # Извлекаем данные
    device_integrity = payload.get('device_integrity', False)
    expires_at_ms = payload.get('expires_at_ms', 0)
    timestamp_ms = payload.get('timestamp_ms', 0)
    ip = payload.get('ip', 'N/A')
    uuid = payload.get('uuid', 'N/A')
    
    # Конвертируем timestamp в datetime
    created_at = datetime.fromtimestamp(timestamp_ms / 1000)
    expires_at = datetime.fromtimestamp(expires_at_ms / 1000)
    now = datetime.now()
    
    # Вычисляем время жизни
    lifetime = expires_at - created_at
    remaining = expires_at - now
    
    # Статус токена
    is_valid = remaining.total_seconds() > 0
    
    print(f"📱 UUID устройства: {uuid}")
    print(f"🌐 IP адрес: {ip}")
    print(f"🔒 Device Integrity: {'✅ Да' if device_integrity else '❌ Нет'}")
    print()
    
    print(f"🕐 Создан: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Истекает: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏳ Время жизни: {format_timedelta(lifetime)}")
    print()
    
    if is_valid:
        print(f"✅ ТОКЕН ДЕЙСТВИТЕЛЕН")
        print(f"   Осталось: {format_timedelta(remaining)}")
        
        # Предупреждение если осталось мало времени
        if remaining.total_seconds() < 600:  # менее 10 минут
            print(f"   ⚠️  ВНИМАНИЕ: Токен скоро истечёт!")
    else:
        print(f"❌ ТОКЕН ИСТЁК")
        print(f"   {format_timedelta(remaining)}")
        print()
        print("🔧 Для обновления токена:")
        print("   1. Откройте сниффер (Charles Proxy / Proxyman)")
        print("   2. Откройте приложение Yandex Go")
        print("   3. Найдите запрос к tc.mobile.yandex.net")
        print("   4. Скопируйте заголовок X-Yandex-Jws")
        print("   5. Обновите config.json")
    
    print()
    print("=" * 70)


if __name__ == '__main__':
    check_token()
