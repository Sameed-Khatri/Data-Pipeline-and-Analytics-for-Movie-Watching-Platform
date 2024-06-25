from faker import Faker
import mysql.connector
from datetime import datetime, timedelta
import random

# Initialize Faker instance
fake = Faker()

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="dwh"
)
cursor = mydb.cursor()
def insert_actors(actors):
    try:
         # Create cursor inside the try block

        # Insert actors into the actor table
        for actor in actors:
            sql = "INSERT INTO actor (name) VALUES (%s)"
            val = (actor,)  # Tuple containing the actor name
            cursor.execute(sql, val)

        mydb.commit()
        print(f"{len(actors)} actors inserted successfully!")

    except mysql.connector.Error as err:
        print("Error:", err)

    finally:
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert actors using the corrected function calls
# Bollywood actors
bollywood_actors = [
    "Akshay Kumar", "Shah Rukh Khan", "Salman Khan", "Aamir Khan", 
    "Hrithik Roshan", "Ranbir Kapoor", "Ajay Devgn", "Amitabh Bachchan", 
    "Ranveer Singh", "Varun Dhawan", "Tiger Shroff", "Vicky Kaushal", 
    "Ayushmann Khurrana", "Shahid Kapoor", "Arjun Kapoor", "Sushant Singh Rajput", 
    "Irrfan Khan", "Kangana Ranaut", "Priyanka Chopra", "Deepika Padukone", 
    "Alia Bhatt", "Kareena Kapoor Khan", "Katrina Kaif", "Anushka Sharma", 
    "Vidya Balan", "Madhuri Dixit", "Sridevi", "Kajol", "Rani Mukerji", 
    "Juhi Chawla", "Karisma Kapoor", "Rekha", "Aishwarya Rai Bachchan", 
    "Sonam Kapoor", "Shraddha Kapoor", "Kriti Sanon", "Jacqueline Fernandez", 
    "Disha Patani", "Kiara Advani", "Bhumi Pednekar", "Taapsee Pannu", 
    "Yami Gautam", "Radhika Apte","Tom Hanks", "Leonardo DiCaprio", "Brad Pitt", "Robert De Niro", 
    "Morgan Freeman", "Denzel Washington", "Al Pacino", "Tom Cruise", 
    "Meryl Streep", "Scarlett Johansson", "Sandra Bullock", "Angelina Jolie", 
    "Emma Watson", "Jennifer Lawrence", "Chris Hemsworth", "Ryan Reynolds", 
    "Will Smith", "Dwayne Johnson", "Chris Pratt", "Mark Wahlberg", 
    "Matthew McConaughey", "Christian Bale", "Eddie Murphy", "Russell Crowe", 
    "Tom Hardy", "Hugh Jackman", "Jake Gyllenhaal", "Ryan Gosling", 
    "Bradley Cooper", "Jennifer Aniston", "Julia Roberts", "Reese Witherspoon", 
    "Charlize Theron", "Halle Berry", "Viola Davis", "Amy Adams", 
    "Emma Stone", "Mila Kunis", "Anne Hathaway", "Lupita Nyong'o", 
    "Saoirse Ronan", "Brie Larson", "Zendaya", "Florence Pugh", 
    "Tessa Thompson"
]
# insert_actors(bollywood_actors)




import mysql.connector

def insert_countries_into_db():
    country_names = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", 
    "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", 
    "Azerbaijan", "The Bahamas", "Bahrain", "Bangladesh", "Barbados", 
    "Belarus", "Belgium", "Belize", "Benin", "Bhutan", 
    "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", 
    "Bulgaria", "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", 
    "Cameroon", "Canada", "Central African Republic", "Chad", "Chile", 
    "China", "Colombia", "Comoros", "Congo, Democratic Republic of the", "Congo, Republic of the", 
    "Costa Rica", "Côte d’Ivoire", "Croatia", "Cuba", 
    "Cyprus", "Czech Republic", "Denmark", "Djibouti", "Dominica", 
    "Dominican Republic", "East Timor (Timor-Leste)", "Ecuador", "Egypt", "El Salvador", 
    "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", 
    "Fiji", "Finland", "France", "Gabon", "The Gambia", 
    "Georgia", "Germany", "Ghana", "Greece", "Grenada", 
    "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", 
    "Honduras", "Hungary", "Iceland", "India", "Indonesia", 
    "Iran", "Iraq", "Ireland", "Italy", 
    "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", 
    "Kiribati", "Korea, North", "Korea, South", "Kosovo", "Kuwait", 
    "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", 
    "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", 
    "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", 
    "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", 
    "Micronesia, Federated States of", "Moldova", "Monaco", "Mongolia", "Montenegro", 
    "Morocco", "Mozambique", "Myanmar (Burma)", "Namibia", "Nauru", 
    "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", 
    "Nigeria", "North Macedonia", "Norway", "Oman", "Pakistan", 
    "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", 
    "Philippines", "Poland", "Portugal", "Qatar", "Romania", 
    "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", 
    "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", 
    "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", 
    "Slovenia", "Solomon Islands", "Somalia", "South Africa", "Spain", 
    "Sri Lanka", "Sudan", "Sudan, South", "Suriname", "Sweden", 
    "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", 
    "Thailand", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", 
    "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", 
    "United Arab Emirates", "United Kingdom", "United States", "Uruguay", 
    "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Vietnam", 
    "Yemen", "Zambia", "Zimbabwe"
    ]
    try:
        # Insert each country into the database
        for country in country_names:
            insert_query = f"INSERT INTO country (countryName) VALUES ('{country}');"
            cursor.execute(insert_query)

        # Commit changes and close connection
        mydb.commit()
        print("Countries inserted successfully into the database.")
    except mysql.connector.Error as err:
        print("Error inserting countries into the database:", err)
    finally:
        cursor.close()
        mydb.close()


# Call the function to insert countries into the database
# insert_countries_into_db()


def insert_age_categories():
    try:
        category_names = ["G", "U", "PG", "PG-13", "12A", "14A", "15+", "16+", "18+", "R", "MA15+", "NC-17"]
        # SQL statement for inserting into the age category table
        sql = "INSERT INTO ageCategory (name) VALUES (%s)"
        
        # Execute the SQL query for each category
        for category_name in category_names:
            # Data to be inserted
            val = (category_name,)
            # Execute the SQL query
            cursor.execute(sql, val)
        
        # Commit changes to the database
        mydb.commit()
        
        print("Age categories inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# List of age categories

# Insert age categories into the database
# insert_age_categories()


def insert_genres():
    try:
        genre_names = ["Action", "Adventure", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi", "Thriller", "Fantasy", "Mystery", "Crime", "Animation", "Family"]
        # SQL statement for inserting into the genre table
        sql = "INSERT INTO genre (genreName) VALUES (%s)"
        
        # Execute the SQL query for each genre
        for genre_name in genre_names:
            # Data to be inserted
            val = (genre_name,)
            # Execute the SQL query
            cursor.execute(sql, val)
        
        # Commit changes to the database
        mydb.commit()
        
        print("Genres inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# List of genres

# Insert genres into the database
# insert_genres()


def insert_languages():
    try:
        
        language_names = ["English", "Spanish", "French", "German", "Mandarin", "Arabic", "Hindi", "Bengali", "Portuguese", "Russian", "Japanese", "Punjabi", "Telugu", "Marathi", "Tamil", "Urdu", "Gujarati", "Kannada", "Odia", "Malayalam"]
        # SQL statement for inserting into the language table
        sql = "INSERT INTO language (language) VALUES (%s)"
        
        # Execute the SQL query for each language
        for language_name in language_names:
            # Data to be inserted
            val = (language_name,)
            # Execute the SQL query
            cursor.execute(sql, val)
        
        # Commit changes to the database
        mydb.commit()
        
        print("Languages inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert languages into the database
# insert_languages()



def insert_creditcard_dummy_data():
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get current date and time
        current_date = datetime.now().date()

        # Add 5 years to the current date for expiration date
        expiration_date = current_date + timedelta(days=365*5)

        # Generate dummy credit card data using Faker
        dummy_data = []
        for _ in range(500):
            cardNumber = fake.credit_card_number(card_type=None)
            cvc = fake.credit_card_security_code(card_type=None)
            dummy_data.append((cardNumber, cvc, expiration_date))

        # SQL statement for inserting into the creditcard table
        sql = "INSERT INTO creditcard (cardNumber, cvc, expirationDate) VALUES (%s, %s, %s)"

        # Execute the SQL query for each set of dummy data
        for cardNumber, cvc, expDate in dummy_data:
            # Data to be inserted
            val = (cardNumber, cvc, expDate)
            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy data inserted into creditcard table successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert dummy data into the creditcard table
# insert_creditcard_dummy_data()
            

def insert_dummy_users():
    try:

        # Initialize Faker instance
        fake = Faker()

        # Get list of country IDs from the database
        cursor.execute("SELECT countryID FROM country")
        country_ids = [row[0] for row in cursor.fetchall()]

        # Get list of credit card IDs from the database
        cursor.execute("SELECT creditCardID FROM creditcard")
        credit_card_ids = [row[0] for row in cursor.fetchall()]

        # Generate dummy data for each user
        for _ in range(500):
            password = fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True)
            email = fake.email()
            countryID = random.choice(country_ids)
            phoneNumber = fake.phone_number()
            creditCardID = random.choice(credit_card_ids)
            name = fake.name()

            # SQL statement for inserting into the user table
            sql = "INSERT INTO user (password, email, countryID, phoneNumber, creditCardID, name) VALUES (%s, %s, %s, %s, %s, %s)"

            # Data to be inserted
            val = (password, email, countryID, phoneNumber, creditCardID, name)

            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print(f"dummy users inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert 500 dummy users into the user table
# insert_dummy_users()



def insert_dummy_plans():
    try:
        plans = [
    ("Basic Plan", 9.99, "3 months"),
    ("Standard Plan", 14.99, "6 months"),
    ("Premium Plan", 19.99, "12 months"),
    ("Family Plan", 24.99, "12 months"),
    ("Student Plan", 7.99, "6 months"),
    ("Ultimate Plan", 29.99, "24 months"),
    ("Weekend Plan", 5.99, "1 month"),
    ("Binge Plan", 17.99, "9 months"),
    ("Movie Buff Plan", 39.99, "24 months"),
    ("Yearly Pass", 49.99, "12 months"),
    ("Monthly Pass", 12.99, "1 month"),
    ("Quarterly Pass", 34.99, "3 months"),
    ("Film Fanatic Plan", 59.99, "36 months"),
    ("Weekday Plan", 4.99, "1 month"),
    ("Silver Plan", 21.99, "12 months"),
    ("Gold Plan", 29.99, "18 months"),
    ("Platinum Plan", 39.99, "24 months"),
    ("Cinema Lover Plan", 49.99, "24 months"),
    ("Streaming Addict Plan", 14.99, "6 months"),
    ("Blockbuster Plan", 64.99, "36 months"),
    ]


        # Generate dummy data for each plan
        for plan in plans:
            planDescription, price, duration = plan

            # SQL statement for inserting into the plan table
            sql = "INSERT INTO plan (planDescription, price, duration) VALUES (%s, %s, %s)"

            # Data to be inserted
            val = (planDescription, price, duration)

            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print(f"{len(plans)} dummy plans inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# List of movie subscription plans [planDescription, price, duration]


# Insert dummy plans into the plan table
# insert_dummy_plans()
            

def insert_dummy_user_profiles(num_users, num_languages):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of language IDs
        language_ids = list(range(1, num_languages + 1))

        # Generate dummy data for each user profile
        for user_id in range(1, num_users + 1):
            # Randomly select a language ID for the user profile
            language_id = random.choice(language_ids)

            # SQL statement for inserting into the user profile table
            sql = "INSERT INTO userProfile (languageID, userID) VALUES (%s, %s)"

            # Data to be inserted
            val = (language_id, user_id)

            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print(f"{num_users} dummy user profiles inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of users and languages
num_users = 500
num_languages = 20

# Insert dummy user profiles into the UserProfile table
# insert_dummy_user_profiles(num_users, num_languages)


def insert_dummy_films(num_films, num_age_categories):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of age category IDs
        age_category_ids = list(range(1, num_age_categories + 1))

        # Generate dummy data for each film
        for _ in range(num_films):
            # Randomly select an age category ID for the film
            age_category_id = random.choice(age_category_ids)

            # Generate other dummy data
            title = fake.sentence(nb_words=3)
            duration = random.randint(60, 180)  # Random duration in minutes
            production_year = fake.year()
            film_description = fake.paragraph(nb_sentences=3)
            add_date = fake.date_this_decade()

            # SQL statement for inserting into the film table
            sql = "INSERT INTO film (title, duration, productionYear, ageCategoryID, filmDescription, addDate) VALUES (%s, %s, %s, %s, %s, %s)"

            # Data to be inserted
            val = (title, duration, production_year, age_category_id, film_description, add_date)

            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print(f"{num_films} dummy films inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of films and age categories
num_films = 1000
num_age_categories = 12

# Insert dummy films into the Film table
# insert_dummy_films(num_films, num_age_categories)


def insert_dummy_film_genre(num_films, num_genres):
    try:

        # Initialize Faker instance
        fake = Faker()

        # Get a list of genre IDs and film IDs
        genre_ids = list(range(1, num_genres + 1))
        film_ids = list(range(1, num_films + 1))

        # Generate dummy data for each film genre
        for film_id in film_ids:
            # Randomly select the number of genres for the film (between 1 and 3)
            num_genres_for_film = random.randint(1, 3)
            
            # Randomly select genre IDs for the film
            film_genre_ids = random.sample(genre_ids, num_genres_for_film)

            # Insert film genre records into the FilmGenre table
            for genre_id in film_genre_ids:
                # SQL statement for inserting into the FilmGenre table
                sql = "INSERT INTO filmGenre (genreID, filmID) VALUES (%s, %s)"

                # Data to be inserted
                val = (genre_id, film_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy film genres inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of films and genres
num_films = 1000
num_genres = 13

# Insert dummy film genres into the FilmGenre table
# insert_dummy_film_genre(num_films, num_genres)


def insert_dummy_film_in_country(num_films, num_countries):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of country IDs and film IDs
        country_ids = list(range(1, num_countries + 1))
        film_ids = list(range(1, num_films + 1))

        # Generate dummy data for each film in country
        for film_id in film_ids:
            # Randomly select the number of countries for the film (between 1 and 5)
            num_countries_for_film = random.randint(1, 5)
            
            # Randomly select country IDs for the film
            film_country_ids = random.sample(country_ids, num_countries_for_film)

            # Insert film in country records into the FilmInCountry table
            for country_id in film_country_ids:
                # SQL statement for inserting into the FilmInCountry table
                sql = "INSERT INTO filmInCountry (filmID,countryID) VALUES (%s, %s)"

                # Data to be inserted
                val = (film_id,country_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy film in country associations inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of films and countries
num_films = 1000
num_countries = 195

# Insert dummy film in country associations into the FilmInCountry table
# insert_dummy_film_in_country(num_films, num_countries)



def insert_dummy_actor_film(num_actors, num_films):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of actor IDs and film IDs
        actor_ids = list(range(1, num_actors + 1))
        film_ids = list(range(1, num_films + 1))

        # Generate dummy data for actor-film associations
        for actor_id in actor_ids:
            # Randomly select the number of films for the actor (between 1 and 10)
            num_films_for_actor = random.randint(1, 10)
            
            # Randomly select film IDs for the actor
            actor_film_ids = random.sample(film_ids, num_films_for_actor)

            # Insert actor-film associations into the ActorFilm table
            for film_id in actor_film_ids:
                # SQL statement for inserting into the ActorFilm table
                sql = "INSERT INTO ActorFilm (actorID, filmID) VALUES (%s, %s)"

                # Data to be inserted
                val = (actor_id, film_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy actor-film associations inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of actors and films
num_actors = 88
num_films = 1000

# Insert dummy actor-film associations into the ActorFilm table
# insert_dummy_actor_film(num_actors, num_films)



def insert_series():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # List of age category IDs (assuming you have 12 age categories)
        age_category_ids = [i for i in range(1, 13)]

        # Insert series into the serie table
        for _ in range(500):  # Assuming you want to insert 1000 series
            # Generate fake data for series
            title = fake.catch_phrase()
            production_year = fake.year()
            age_category_id = random.choice(age_category_ids)
            seasons = random.randint(1, 10)  # Assuming each series has 1 to 10 seasons
            description = fake.text()
            add_date = fake.date_this_decade()

            # SQL statement for inserting into the serie table
            sql = "INSERT INTO serie (title, productionYear, ageCategoryID, seasons, serieDescription, addDate) VALUES (%s, %s, %s, %s, %s, %s)"
            
            # Data to be inserted
            val = (title, production_year, age_category_id, seasons, description, add_date)
            
            # Execute the SQL query
            cursor.execute(sql, val)
        
        # Commit changes to the database
        mydb.commit()
        
        print("Series inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert series into the database
# insert_series()



def insert_dummy_film_genre(num_series, num_genres):
    try:

        # Initialize Faker instance
        fake = Faker()

        # Get a list of genre IDs and film IDs
        genre_ids = list(range(1, num_genres + 1))
        serie_ids = list(range(1, num_series + 1))

        # Generate dummy data for each film genre
        for serie_id in serie_ids:
            # Randomly select the number of genres for the film (between 1 and 3)
            num_genres_for_serie = random.randint(1, 3)
            
            # Randomly select genre IDs for the film
            serie_genre_ids = random.sample(genre_ids, num_genres_for_serie)

            # Insert film genre records into the FilmGenre table
            for genre_id in serie_genre_ids:
                # SQL statement for inserting into the FilmGenre table
                sql = "INSERT INTO serieGenre (genreID, serieID) VALUES (%s, %s)"

                # Data to be inserted
                val = (genre_id, serie_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy serie genres inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of films and genres
num_series = 500
num_genres = 13

# Insert dummy film genres into the FilmGenre table
# insert_dummy_film_genre(num_series, num_genres)


def insert_dummy_serie_in_country(num_series, num_countries):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of country IDs and film IDs
        country_ids = list(range(1, num_countries + 1))
        serie_ids = list(range(1, num_series + 1))

        # Generate dummy data for each film in country
        for serie_id in serie_ids:
            # Randomly select the number of countries for the film (between 1 and 5)
            num_countries_for_serie = random.randint(1, 5)
            
            # Randomly select country IDs for the film
            serie_country_ids = random.sample(country_ids, num_countries_for_serie)

            # Insert film in country records into the FilmInCountry table
            for country_id in serie_country_ids:
                # SQL statement for inserting into the FilmInCountry table
                sql = "INSERT INTO serieInCountry (serieID,countryID) VALUES (%s, %s)"

                # Data to be inserted
                val = (serie_id,country_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy serie in country associations inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of films and countries
num_series = 500
num_countries = 195

# Insert dummy film in country associations into the FilmInCountry table
# insert_dummy_serie_in_country(num_series, num_countries)

# for serie table
def insert_dummy_actor_film(num_actors, num_films):
    try:
        
        # Initialize Faker instance
        fake = Faker()

        # Get a list of actor IDs and film IDs
        actor_ids = list(range(1, num_actors + 1))
        film_ids = list(range(1, num_films + 1))

        # Generate dummy data for actor-film associations
        for actor_id in actor_ids:
            # Randomly select the number of films for the actor (between 1 and 10)
            num_films_for_actor = random.randint(1, 10)
            
            # Randomly select film IDs for the actor
            actor_film_ids = random.sample(film_ids, num_films_for_actor)

            # Insert actor-film associations into the ActorFilm table
            for film_id in actor_film_ids:
                # SQL statement for inserting into the ActorFilm table
                sql = "INSERT INTO actorserie (actorID, serieID) VALUES (%s, %s)"

                # Data to be inserted
                val = (actor_id, film_id)

                # Execute the SQL query
                cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()

        print("Dummy actor-film associations inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Number of actors and films
num_actors = 88
num_films = 500

# Insert dummy actor-film associations into the ActorFilm table
# insert_dummy_actor_film(num_actors, num_films)



def insert_episodes():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # Get list of series IDs from the database
        cursor.execute("SELECT serieID FROM serie")
        series_ids = [row[0] for row in cursor.fetchall()]

        # Initialize counter for total number of episodes
        total_episodes = 0

        # Insert episodes into the episode table
        for serie_id in series_ids:
            # Generate a random number of seasons for the series (1 to 10)
            num_seasons = random.randint(1, 10)
            for season_number in range(1, num_seasons + 1):
                # Generate a random number of episodes for each season (1 to 20)
                num_episodes = random.randint(1, 20)
                for episode_number in range(1, num_episodes + 1):
                    # Check if total number of episodes has reached 1000
                    if total_episodes >= 1000:
                        break  # Exit loop if 1000 episodes reached
                    
                    # Generate fake data for each episode
                    episode_title = fake.catch_phrase()
                    episode_description = fake.text()
                    episode_duration = random.randint(20, 60)  # Assuming episode duration in minutes

                    # SQL statement for inserting into the episode table
                    sql = "INSERT INTO episode (episodeTitle, seasonNumber, episodeDescription, serieID, episodeDuration) VALUES (%s, %s, %s, %s, %s)"
                    
                    # Data to be inserted
                    val = (episode_title, season_number, episode_description, serie_id, episode_duration)
                    
                    # Execute the SQL query
                    cursor.execute(sql, val)
                    
                    # Increment total episodes counter
                    total_episodes += 1
        
        # Commit changes to the database
        mydb.commit()
        
        print("Episodes inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert episodes into the database
# insert_episodes()
            

def insert_film_wishlist():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # Get list of film IDs from the database
        cursor.execute("SELECT filmID FROM film")
        film_ids = [row[0] for row in cursor.fetchall()]

        # Get list of user profile IDs from the database
        cursor.execute("SELECT userProfileID FROM userProfile")
        user_profile_ids = [row[0] for row in cursor.fetchall()]

        # Shuffle the film IDs and user profile IDs
        random.shuffle(film_ids)
        random.shuffle(user_profile_ids)

        # Insert film wishlist entries into the film_wishlist table
        for i in range(100):
            film_id = film_ids[i % len(film_ids)]
            user_profile_id = user_profile_ids[i % len(user_profile_ids)]

            # SQL statement for inserting into the film_wishlist table
            sql = "INSERT INTO filmwhishlist (filmID, userProfileID) VALUES (%s, %s)"
            
            # Data to be inserted
            val = (film_id, user_profile_id)
            
            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()
        
        print("Film wishlist entries inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert film wishlist entries into the database
# insert_film_wishlist()
            
# for serie wishlist
def insert_film_wishlist():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # Get list of film IDs from the database
        cursor.execute("SELECT serieID FROM serie")
        film_ids = [row[0] for row in cursor.fetchall()]

        # Get list of user profile IDs from the database
        cursor.execute("SELECT userProfileID FROM userProfile")
        user_profile_ids = [row[0] for row in cursor.fetchall()]

        # Shuffle the film IDs and user profile IDs
        random.shuffle(film_ids)
        random.shuffle(user_profile_ids)

        # Insert film wishlist entries into the film_wishlist table
        for i in range(100):
            film_id = film_ids[i % len(film_ids)]
            user_profile_id = user_profile_ids[i % len(user_profile_ids)]

            # SQL statement for inserting into the film_wishlist table
            sql = "INSERT INTO seriewhishlist (serieID, userProfileID) VALUES (%s, %s)"
            
            # Data to be inserted
            val = (film_id, user_profile_id)
            
            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()
        
        print("Film wishlist entries inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert film wishlist entries into the database
# insert_film_wishlist()
            


def insert_watching_history():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # Get list of episode IDs from the database
        cursor.execute("SELECT episodeID FROM episode")
        episode_ids = [row[0] for row in cursor.fetchall()]

        # Get list of film IDs from the database
        cursor.execute("SELECT filmID FROM film")
        film_ids = [row[0] for row in cursor.fetchall()]

        # Get list of user profile IDs from the database
        cursor.execute("SELECT userProfileID FROM userProfile")
        user_profile_ids = [row[0] for row in cursor.fetchall()]

        # Shuffle the episode IDs, film IDs, and user profile IDs
        random.shuffle(episode_ids)
        random.shuffle(film_ids)
        random.shuffle(user_profile_ids)

        # Insert watching history entries into the watching_history table
        for i in range(150):
            user_profile_id = user_profile_ids[i % len(user_profile_ids)]
            watch_date = fake.date_time_this_year(before_now=True, after_now=False, tzinfo=None)

            # Decide whether to insert an episode or a film
            if random.choice([True, False]):
                episode_id = random.choice(episode_ids)
                film_id = None
            else:
                film_id = random.choice(film_ids)
                episode_id = None

            # SQL statement for inserting into the watching_history table
            sql = "INSERT INTO watchinghistory (userProfileID, episodeID, filmID, watchDate) VALUES (%s, %s, %s, %s)"
            
            # Data to be inserted
            val = (user_profile_id, episode_id, film_id, watch_date)
            
            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()
        
        print("Watching history entries inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert watching history entries into the database
# insert_watching_history()
            

def insert_payments():
    try:
        
        # Initialize Faker generator
        fake = Faker()

        # Get list of user IDs from the database
        cursor.execute("SELECT userID FROM user")
        user_ids = [row[0] for row in cursor.fetchall()]

        # Get list of plan IDs from the database
        cursor.execute("SELECT planID FROM plan")
        plan_ids = [row[0] for row in cursor.fetchall()]

        # Insert payment entries into the payment table
        for user_id in user_ids:
            # Generate random payment date within the last year
            payment_date = fake.date_time_this_year(before_now=True, after_now=False, tzinfo=None)
            
            # Generate random expiry date, 1 to 12 months ahead of payment date
            expiry_date = payment_date + timedelta(days=random.randint(30, 365))

            # Randomly select a plan for the user
            plan_id = random.choice(plan_ids)

            # SQL statement for inserting into the payment table
            sql = "INSERT INTO payment (userID, planID, paymentDate, expiryDate) VALUES (%s, %s, %s, %s)"
            
            # Data to be inserted
            val = (user_id, plan_id, payment_date, expiry_date)
            
            # Execute the SQL query
            cursor.execute(sql, val)

        # Commit changes to the database
        mydb.commit()
        
        print("Payment entries inserted successfully!")

    except mysql.connector.Error as err:
        # Handle any errors that occur during execution
        print("Error:", err)

    finally:
        # Close database connection
        if mydb.is_connected():
            cursor.close()
            mydb.close()

# Insert payment entries into the database
insert_payments()