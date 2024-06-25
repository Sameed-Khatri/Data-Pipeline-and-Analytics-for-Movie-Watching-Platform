import mysql.connector
import random
from datetime import datetime, timedelta

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="dwh"
)
cursor = mydb.cursor()

# Fetch episode data with addDate via a join with the serie table
def get_episode_data():
    query = """
    SELECT e.episodeID, e.serieID, e.episodeDuration, s.addDate
    FROM episode e
    JOIN serie s ON e.serieID = s.serieID;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return [{'episodeID': row[0], 'serieID': row[1], 'duration': int(row[2]), 'addDate': row[3].strftime('%Y-%m-%d')} for row in rows]

# Fetch film data
def get_film_data():
    query = """
    SELECT filmID, duration, addDate
    FROM film;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return [{'filmID': row[0], 'duration': int(row[1]), 'addDate': row[2].strftime('%Y-%m-%d')} for row in rows]

# Function to generate random timestamps
def random_timestamp(start_date, end_date):
    delta = end_date - start_date
    random_second = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_second)

# Function to generate random timestamps within 2024
def random_timestamp_2024():
    start_date = datetime.strptime('2024-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-12-31', '%Y-%m-%d')
    return random_timestamp(start_date, end_date)

# Generate exactly 3000 watching history data for episodes
def generate_episode_history(episode_data):
    watching_history = []
    for _ in range(3000):  # Generate exactly 3000 entries
        episode = random.choice(episode_data)  # Randomly select an episode
        user_id = random.randint(1, 500)
        rating = random.randint(1, 5)
        start_time = random_timestamp_2024()
        duration_variation = random.randint(-15, 15)
        end_time = start_time + timedelta(minutes=(episode['duration'] + duration_variation))
        
        watching_history.append({
            'userProfileID': user_id,
            'episodeID': episode['episodeID'],
            'filmID': None,
            'ratings': rating,
            'startTime': start_time,
            'endTime': end_time
        })
    return watching_history

# Generate exactly 3000 watching history data for films
def generate_film_history(film_data):
    watching_history = []
    for _ in range(3000):  # Generate exactly 3000 entries
        film = random.choice(film_data)  # Randomly select a film
        user_id = random.randint(1, 500)
        rating = random.randint(1, 5)
        start_time = random_timestamp_2024()
        duration_variation = random.randint(-30, 30)
        end_time = start_time + timedelta(minutes=(film['duration'] + duration_variation))
        
        watching_history.append({
            'userProfileID': user_id,
            'episodeID': None,
            'filmID': film['filmID'],
            'ratings': rating,
            'startTime': start_time,
            'endTime': end_time
        })
    return watching_history

# Aggregate the results
all_episode_data = get_episode_data()
all_film_data = get_film_data()
history_episode = generate_episode_history(all_episode_data)
history_film = generate_film_history(all_film_data)

# Combine the two lists into one
final_history = history_episode + history_film

# Insert final_history into watchinghistory table
insert_query = """
INSERT INTO watchinghistory (userProfileID, episodeID, filmID, ratings, startTime, endTime)
VALUES (%s, %s, %s, %s, %s, %s);
"""

# Insert each record
for record in final_history:
    cursor.execute(insert_query, (
        record['userProfileID'],
        record['episodeID'],
        record['filmID'],
        record['ratings'],
        record['startTime'].strftime('%Y-%m-%d %H:%M:%S'),
        record['endTime'].strftime('%Y-%m-%d %H:%M:%S')
    ))

# Commit the transaction
mydb.commit()

# Clean up resources
cursor.close()
mydb.close()
