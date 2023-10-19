from connector import conn

cursor = conn.cursor()
# Create the Subscription table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Expenses (
    date DATETIME,
    USD DECIMAL(10, 4),
    rate DECIMAL(10, 4),
    CAD DECIMAL(10, 4)
)
""")
