import os
from time import sleep
from json import dumps
from kafka import KafkaProducer

from faker import Faker
from faker.providers import bank
import random

fake = Faker()
fake.add_provider(bank)

# Simple Kafka on docker compose so same host for all brokers
KAFKA_IP = os.environ.get('KAFKA_IP', 'localhost')

class FakePerson:
    def __init__(self):
        rand = random.randint(0,8)
        if rand <= 2:
            self.male()
        elif rand <= 5:
            self.female()
        else:
            self.nonbinary()
    
        if (rand % 2) == 0:
            self.maritalstatus = "M"
        else:
            self.maritalstatus = "S"

    def male(self):
        self.gender = "M"
        self.prefix = fake.prefix_male()
        self.first_name = fake.first_name_male()
        self.last_name = fake.last_name_male()
        self.middle_name = fake.first_name_male()

    def female(self):
        self.gender = "F"
        self.prefix = fake.prefix_female()
        self.first_name = fake.first_name_female()
        self.last_name = fake.last_name_female()
        self.middle_name = fake.first_name_female()

    def nonbinary(self):
        self.gender = "N"
        self.prefix = fake.prefix_nonbinary()
        self.first_name = fake.first_name_nonbinary()
        self.last_name = fake.last_name_nonbinary()
        self.middle_name = fake.first_name_nonbinary()

def fake_customer(index, person):
    return {
            "cust_num": f"{index}",
            "status": "a",
            "name": f"{person.prefix} {person.first_name} {person.last_name}",
            "fname": person.first_name,
            "lname": person.last_name,
            "mnames": person.middle_name,
            "form": person.prefix,
            "sex": person.gender,
            "maritalstatus": person.maritalstatus
        }

def fake_account(index, person):
    return {
            "created": f"{fake.date_between(start_date='-20y', end_date='today')}",
            "valid_from": None,
            "valid_to": None,
            "cust_num": f"{index}",
            "acct_num": f"00-{fake.aba()}-00",
            "status": "A",
            "type": "Test",
            "name": f"{person.first_name}'s Account",
            "pin": "1234"
        }

def send(index_start, count):
    producer = KafkaProducer(
        bootstrap_servers=[f'{KAFKA_IP}:9093', f'{KAFKA_IP}:9094', f'{KAFKA_IP}:9095'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    for index in range(index_start, index_start+count):
        print("Iteration", index)

        person = FakePerson()
    
        producer.send('raw_table_cust', value=fake_customer(index, person))
        producer.send('raw_table_acct', value=fake_account(index, person))
        sleep(0.5)

if __name__ == "__main__":
    send(0,10)
