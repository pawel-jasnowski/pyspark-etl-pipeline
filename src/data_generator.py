from faker import Faker
from datetime import datetime, timedelta
import random
import os
import csv
import uuid # Universally Unique Identifier ID generator
from config import HIGH_RISK_COUNTRIES

fake = Faker()

def random_timestamp_generator():

    now = datetime.now()
    start = now - timedelta(days=1)

    time_diff = now - start
    total_sec = time_diff.total_seconds()
    random_sec = random.uniform(0, total_sec)
    random_datetime = start + timedelta(seconds=random_sec)

    return random_datetime.isoformat()

def transaction_generator(customer_ids):
    """ transaction_ID , customer_id, amount, countru_code, timstamp"""

    customer_id = random.choice(customer_ids) # from the customer list
    is_suspicious = random.random() < 0.03  #less than 3%

    if is_suspicious:  # transaction is suspicious

        anomaly_type = random.choice(['amount', 'risk_country'])

        if anomaly_type == 'amount':
            amount = round(random.uniform(15000.0, 50000.0), 2)
            country_code = fake.country_code()

        else:
            amount = round(random.uniform(100.0, 5000.0), 2)
            country_code = random.choice(HIGH_RISK_COUNTRIES)

    else: # transaction is not suspicious
        amount = round(random.uniform(10.0, 1000.0),2)
        country_code = fake.country_code()

    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': str(customer_id),
        'amount': amount,
        'currency': 'PLN',
        'country_code': country_code,
        'timestamp': random_timestamp_generator()
    }

def main(num_transactions = 1000):
    """ main transaction generator"""

    customer_ids = [str(uuid.uuid4()) for _ in range(100)]          #list of customers
    transactions = [transaction_generator(customer_ids) for _ in range(num_transactions)]   # transaction generator call

    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok = True)

    filename = f'TRANSACTIONS_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    filepath = os.path.join(output_dir, filename)

    headers = ['transaction_id', 'customer_id', 'amount',  'currency', 'country_code', 'timestamp']

    with open(filepath, 'w', newline ='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter ='|')
        writer.writeheader()
        writer.writerows(transactions)

    print(f'Transaction file was generated with {num_transactions} transactions to `{filepath}`')

if __name__ == "__main__":

    main()
