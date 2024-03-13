if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from kafka import KafkaProducer
import random
import json
import time
from faker import Faker
from faker.providers import internet, misc, person, date_time, address, user_agent
import hashlib

from default_repo.utils.helper.name_list import NAME_LIST
from default_repo.utils.helper.event_properties import ACTIVITY_USER_PROPERTIES
import default_repo.utils.helper.constants as CONSTANTS

fake = Faker()
fake.add_provider(internet)
fake.add_provider(misc)
fake.add_provider(person)
fake.add_provider(date_time)
fake.add_provider(address)
fake.add_provider(user_agent)


topic = 'hamropatro.event.analytics'
producer = KafkaProducer(
    bootstrap_servers='182.93.94.251:9092',
    batch_size=32768,
)

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    json_data = generate_data(30000)
    print(json_data)
    # publish_messages(data)
    # producer.flush()
    # Specify your data exporting logic here

def generate_data(num_rows):
    user_names = NAME_LIST
    event_properties = ACTIVITY_USER_PROPERTIES
    data = []
    # numberPushed = 0

    for row in range(num_rows):
        userName = random.choice(user_names)
        featureName = random.choice(CONSTANTS.FEATURE_LIST)
        osTypes = random.choice(CONSTANTS.OS_TYPES_LIST)
        country = fake.country()
        
        current_time_millis = int(time.time() * 1000)

        data.append ( {
            "appId": "hamropatro-android",
            "appVersion": random.choice(CONSTANTS.APP_VERSION_LIST),
            "userName": userName,
            "userId": hashlib.md5(userName.encode()).hexdigest(),
            "userImage": "https://www.hamropatro.com/images/hamropatro.png",
            "ipAddress": fake.ipv4(),
            "city": fake.city(),
            "country": country,
            "countryISO": fake.country_code(),
            "deviceType": random.choice(CONSTANTS.DEVICE_TYPE_LIST),
            "deviceBrand": random.choice(CONSTANTS.DEVICE_BRAND_LIST),
            "os": osTypes,
            "osVersion": random.choice(CONSTANTS.OS_VERSION_LIST),
            "feature": featureName,
            "eventName": random.choice(CONSTANTS.EVENT_NAME_LIST),
            "eventTimeStamp": random.randint(1708942378677, 1708942978677),
            "serviceName": "HAMROPATRO_APP",
            "source": random.choice(CONSTANTS.SOURCE_LIST),
            "eventProperties": random.choice(event_properties),
            "created": current_time_millis,
            "updated": current_time_millis,
            "sessionId": hashlib.md5((featureName+osTypes).encode()).hexdigest(),
            "deviceId": hashlib.md5((featureName+osTypes).encode()).hexdigest()
        })
        
        
        # numberPushed = numberPushed + 1
    # print(numberPushed)
    # numberPushed = 0
    print(len(data))
    for x in data:
        producer.send(topic, value=json.dumps(x).encode('utf-8'), key=x.get("userName").encode('utf-8'))

# time.sleep(40)
producer.flush()