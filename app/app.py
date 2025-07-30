from flask import Flask, jsonify
import mysql.connector

app = Flask(__name__)

DB_CONFIG = {
    'host': 'mysql',  # service name in docker-compose
    'user': 'airflow',
    'password': 'airflow',
    'database': 'airflow',
}

@app.route('/')
def home():
    return "Flask API is running!"

@app.route('/shows-count')
def shows_count():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM shows")
        count = cursor.fetchone()[0]
        return jsonify({'shows_count': count})
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        if conn:
            conn.close()


@app.route('/analytics/genre_count')
def genre_count():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
    SELECT g.genre_name, COUNT(sg.show_id) 
    FROM genres g
    JOIN show_genres sg ON g.genre_id = sg.genre_id
    GROUP BY g.genre_name
    ORDER BY COUNT(sg.show_id) DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{ "genre": row[0], "count": row[1]} for row in results])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
