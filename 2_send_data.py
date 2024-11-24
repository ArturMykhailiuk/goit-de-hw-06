## 1 ##
# Генерація потоку даних
from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random


def send_data(id_sensor):
    # Створення Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Назва топіку
    my_name = "artur_home"
    building_sensors_in = 'artur_home_building_sensors_in'

    try:
        data = {
            'sensor_id': id_sensor,
            'timestamp': int(time.time()),
            'temperature': random.uniform(25, 45),
            'humidity': random.uniform(15, 85)
        }
        producer.send(building_sensors_in, key=str(id_sensor), value=json.dumps(data))

        print(f"Sent data: {data} to topic '{building_sensors_in}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.flush()
        producer.close()  # Закриття producer


if __name__ == "__main__":
    sensor_id = random.randint(1000, 9999)
    send_data(sensor_id)
    sensor_id = random.randint(1000, 9999)
    send_data(sensor_id)
    sensor_id = random.randint(1000, 9999)
    send_data(sensor_id)
    sensor_id = random.randint(1000, 9999)
    send_data(sensor_id)
    sensor_id = random.randint(1000, 9999)
    send_data(sensor_id)



