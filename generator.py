import time
import random
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from sklearn.linear_model import LogisticRegression

# 🔌 MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your password",
    database="fraud_db",
    auth_plugin='mysql_native_password'
)

cursor = conn.cursor()

locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami', 'Dallas', 'Atlanta']

# 🧠 Train ML model
train_data = pd.DataFrame({
    "amount": [50, 120, 300, 450, 800, 1100, 1500],
    "user_id": [1, 2, 3, 4, 5, 6, 7],
    "is_fraud": [0, 0, 0, 0, 0, 1, 1]
})

X = train_data[["amount", "user_id"]]
y = train_data["is_fraud"]

model = LogisticRegression()
model.fit(X, y)

# 🔹 Generate one transaction
def generate_transaction():
    # Random date within last 30 days
    random_days = random.randint(0, 30)
    
    # Random seconds in a day
    random_seconds = random.randint(0, 86400)

    random_time = datetime.now() - timedelta(days=random_days, seconds=random_seconds)

    # 95% normal, 5% suspicious
    if random.random() < 0.97:
        amount = round(random.uniform(10, 500), 2)
    else:
        amount = round(random.uniform(900, 1500), 2)

    return {
        "user_id": random.randint(1, 5000),
        "amount": amount,
        "location": random.choice(locations),
        "timestamp": random_time.strftime('%Y-%m-%d %H:%M:%S')
    }
# 🔹 Fraud rules
def detect_fraud(df):
    df['is_fraud'] = 0

    # Rule 1: Very high amount
    df.loc[df['amount'] > 1000, 'is_fraud'] = 1

    # Rule 2: High frequency + high amount (combined condition)
    counts = df['user_id'].value_counts()
    suspicious_users = counts[counts > 20].index

    df.loc[
        (df['user_id'].isin(suspicious_users)) & (df['amount'] > 700),
        'is_fraud'
    ] = 1

    return df
# 🚀 BULK GENERATION
BATCH_SIZE = 1000   # 1000 rows per batch
TOTAL_ROWS = 10000  # total rows

all_data = []

for _ in range(TOTAL_ROWS):
    all_data.append(generate_transaction())

df = pd.DataFrame(all_data)

# Apply fraud logic
df = detect_fraud(df)

# ML prediction
df["ml_prediction"] = model.predict(df[["amount", "user_id"]])

# 🗄️ Insert into MySQL (FAST)
sql = """
INSERT INTO transactions (user_id, amount, location, timestamp, is_fraud)
VALUES (%s, %s, %s, %s, %s)
"""

values = [
    (int(row.user_id), float(row.amount), row.location, row.timestamp, int(row.is_fraud))
    for row in df.itertuples()
]

cursor.executemany(sql, values)
conn.commit()

print(f"✅ Inserted {len(values)} rows successfully!")