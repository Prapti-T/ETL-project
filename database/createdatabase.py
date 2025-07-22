import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='password',
    database='netflix',
    autocommit= True,
)

cursor = conn.cursor()

# cursor.execute("CREATE DATABASE IF NOT EXISTS netflix")
# conn.commit()

# Create tables
create_shows_table = """
CREATE TABLE IF NOT EXISTS shows (
    show_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(20),
    title VARCHAR(255),
    release_year INT,
    rating VARCHAR(20),
    duration VARCHAR(50),
    date_added DATE,
    description TEXT
);
"""

create_directors_table = """
CREATE TABLE IF NOT EXISTS directors (
    director_id INT AUTO_INCREMENT PRIMARY KEY,
    director_name VARCHAR(255) UNIQUE
);
"""

create_show_directors_table = """
CREATE TABLE IF NOT EXISTS show_directors (
    show_id VARCHAR(50),
    director_id INT,
    PRIMARY KEY (show_id, director_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE
);
"""

create_actors_table = """
CREATE TABLE IF NOT EXISTS actors (
    actor_id INT AUTO_INCREMENT PRIMARY KEY,
    actor_name VARCHAR(255) UNIQUE
);
"""

create_show_actors_table = """
CREATE TABLE IF NOT EXISTS show_actors (
    show_id VARCHAR(50),
    actor_id INT,
    PRIMARY KEY (show_id, actor_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id) REFERENCES actors(actor_id) ON DELETE CASCADE
);
"""

create_countries_table = """
CREATE TABLE IF NOT EXISTS countries (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(255) UNIQUE
);
"""

create_show_countries_table = """
CREATE TABLE IF NOT EXISTS show_countries (
    show_id VARCHAR(50),
    country_id INT,
    PRIMARY KEY (show_id, country_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (country_id) REFERENCES countries(country_id) ON DELETE CASCADE
);
"""

create_genres_table = """
CREATE TABLE IF NOT EXISTS genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(255) UNIQUE
);
"""

create_show_genres_table = """
CREATE TABLE IF NOT EXISTS show_genres (
    show_id VARCHAR(50),
    genre_id INT,
    PRIMARY KEY (show_id, genre_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);
"""

#Execute all tables
tables = [
    create_shows_table,
    create_directors_table,
    create_show_directors_table,
    create_actors_table,
    create_show_actors_table,
    create_countries_table,
    create_show_countries_table,
    create_genres_table,
    create_show_genres_table,
]

for query in tables:
    cursor.execute(query)
    print("Executed:", query.split('(')[0].strip())

cursor.close()
conn.close()

print("All tables created successfully in MySQL database 'netflix'.")