import os
import requests
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Загрузка переменных окружения из .env файла
load_dotenv()

# Данные для API из файла .env
API_KEY = os.getenv('API_KEY')
API_USERNAME = os.getenv('API_USERNAME')
BASE_URL = 'https://support.wirenboard.com'

# Параметры для подключения к InfluxDB
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Пользователи для проверки назначенных тем
usernames = ['dust', 'bringer', 'aleksandr_khlebnikov', 'BrainRoot', 'Because']

# Заголовки для API
headers = {
    "Accept": "application/json",
    "Api-Key": API_KEY,
    "Api-Username": API_USERNAME,
    "Cache-Control": "no-cache"
}

# Функция для получения количества назначенных тем
def count_assigned_topics(username):
    total_topics = 0
    page = 0
    more_pages = True

    while more_pages:
        url = f"{BASE_URL}/latest.json?assigned={username}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            topics = data.get('topic_list', {}).get('topics', [])
            
            if topics:
                total_topics += len(topics)
            else:
                more_pages = False
            page += 1
        else:
            print(f"Ошибка: {response.status_code}, {response.text}")
            more_pages = False
    
    return total_topics

# Переменная для хранения общего количества назначенных тем
total_all_users = 0

# Подключение к InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Обработка списка пользователей и вывод количества назначенных тем
for username in usernames:
    topic_count = count_assigned_topics(username)
    total_all_users += topic_count
    print(f"Пользователь {username}: {topic_count} назначенных тем.")
    
    # Создание и отправка данных в InfluxDB
    point = Point("api_assigned") \
        .tag("username", username) \
        .field("assigned_topics", topic_count)
    
    write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# Отправка итогового количества тем в InfluxDB
point_total = Point("api_assigned") \
    .tag("username", "итого") \
    .field("assigned_topics", total_all_users)

write_api.write(bucket=INFLUXDB_BUCKET, record=point_total)

# Вывод общего количества назначенных тем
print(f"Пользователь итого: {total_all_users} назначенных тем.")

# Закрытие клиента
client.close()

# Вывод завершённого сообщения
print("Вывод количества назначенных тем завершён.")
