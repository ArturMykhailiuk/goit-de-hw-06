from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нових топіків
my_name = "artur_home"
building_sensors_in = f'{my_name}_building_sensors_in'
building_sensors_out = f'{my_name}_building_sensors_out'
num_partitions = 2
replication_factor = 1

building_sensors_topic_in = NewTopic(name=building_sensors_in, num_partitions=num_partitions, replication_factor=replication_factor)
building_sensors_topic_out = NewTopic(name=building_sensors_out, num_partitions=num_partitions, replication_factor=replication_factor)

# Видалення старих топіків
try:
    admin_client.delete_topics(topics=['artur_home_building_sensors_in', 'artur_home_building_sensors_out'])
    print(f"Топіки успішно видалено.")
except Exception as e:
    print(f"Помилка при видаленні топіку: {e}")


# Створення нових топіків
try:
    admin_client.create_topics(new_topics=[building_sensors_topic_in, building_sensors_topic_out], validate_only=False)
    print(f"Topic '{building_sensors_in}' created successfully.")
    print(f"Topic '{building_sensors_out}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
for topic in admin_client.list_topics():
    if my_name in topic:
        print(f"Topic '{topic}' already exists.")

# Закриття зв'язку з клієнтом
admin_client.close()

