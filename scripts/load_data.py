import pandas as pd
from sqlalchemy import create_engine

# Read CSV - marketing data set
df = pd.read_csv('data/raw/bank.csv')

# Create DB
engine = create_engine('sqlite:///data/banking.db')

# Load to table
df.to_sql('transactions', engine, if_exists='replace', index=False)
print("Data loaded successfully into the database.")

df = pd.read_sql("SELECT * FROM transactions LIMIT 10", engine)
print(df)

# Close the engine
# engine.dispose()
# print("Database connection closed.")
