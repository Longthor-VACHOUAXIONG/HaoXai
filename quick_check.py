import sqlite3

conn = sqlite3.connect('uploads/CAN2.db')
cursor = conn.cursor()

# Check sample IDs in screening table
cursor.execute('SELECT sample_id FROM screening LIMIT 20')
screening_samples = [row[0] for row in cursor.fetchall()]
print('Sample IDs in screening table (first 20):')
for i, sample_id in enumerate(screening_samples, 1):
    print(f'{i:2d}. {sample_id}')

conn.close()
