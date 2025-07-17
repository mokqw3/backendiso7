import os
import requests
import json
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
from dotenv import load_dotenv # Used for local development
from urllib.parse import urlparse # Import the URL parsing library

# Load environment variables from .env file (for local testing)
load_dotenv()

# --- Configuration ---
# Get the database URL from the environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("No DATABASE_URL set. Please set it in your .env file locally or in Render's environment variables.")

# ** NEW, MORE ROBUST FIX **
# This block programmatically rebuilds the URL to ensure SQLAlchemy can parse it.
try:
    parsed_url = urlparse(DATABASE_URL)
    # Create a new URL object in the format SQLAlchemy prefers
    db_url_object = f"postgresql+psycopg2://{parsed_url.username}:{parsed_url.password}@{parsed_url.hostname}:{parsed_url.port or 5432}/{parsed_url.path[1:]}"
    if parsed_url.query:
        db_url_object += f"?{parsed_url.query}"
except Exception as e:
    raise ValueError(f"Could not process the DATABASE_URL. Error: {e}")


# Initialize Flask App and SQLAlchemy
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = db_url_object
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Database Model ---
# This class defines the structure of our database table.
class KbtResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    period = db.Column(db.String(20), unique=True, nullable=False)
    number = db.Column(db.String(10), nullable=False)
    color = db.Column(db.String(10), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<Result {self.period}>'

# --- API Fetching and Database Logic ---
def fetch_and_store_data():
    """
    Fetches data from the API and stores new results in the database.
    This function is run by the background scheduler.
    """
    with app.app_context():
        print("Scheduler: Fetching data from API...")
        api_url = "https://kbtpredictor.shop/API/1_min.php"
        response = None # Define response here to access it in except block
        try:
            response = requests.get(api_url, timeout=15)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list) and data:
                new_results_found = 0
                for item in data:
                    period_id = item.get('period')
                    # Check if a result with this period already exists
                    exists = KbtResult.query.filter_by(period=period_id).first()
                    if not exists and period_id is not None:
                        # If it doesn't exist, create and save the new result
                        new_result = KbtResult(
                            period=str(period_id), # Ensure period is a string
                            number=str(item.get('number', 'N/A')),
                            color=str(item.get('color', 'N/A'))
                        )
                        db.session.add(new_result)
                        new_results_found += 1
                
                if new_results_found > 0:
                    db.session.commit()
                    print(f"Scheduler: Successfully stored {new_results_found} new results.")
                else:
                    print("Scheduler: No new results to store.")
            else:
                print("Scheduler: API response was not a valid list or was empty.")
                if response:
                    print(f"Scheduler: Raw response content: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Scheduler Error: A network error occurred: {e}")
            db.session.rollback()
        except json.JSONDecodeError:
            print("Scheduler Error: Failed to decode JSON from API response.")
            if response:
                print(f"Scheduler: Raw response content that caused error: {response.text}")
            db.session.rollback()
        except Exception as e:
            print(f"Scheduler Error: An unexpected error occurred: {e}")
            db.session.rollback() # Rollback any partial changes on error

# --- Flask Routes ---
@app.route('/')
def index():
    """
    The main page. Fetches the last 100 results from our database and displays them.
    """
    error = None
    results = []
    try:
        # Query our database for the last 100 results, ordered by period descending
        results = KbtResult.query.order_by(KbtResult.period.desc()).limit(100).all()
    except Exception as e:
        error = f"Could not connect to the database or query results: {e}"
        print(f"Error rendering page: {e}")

    # Get current time in Indian Standard Time (IST)
    ist = pytz.timezone('Asia/Kolkata')
    last_updated = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S %Z')
    
    return render_template('index.html', results=results, error=error, last_updated=last_updated)

# --- App Initialization ---
# This block ensures that the app context is available for the first run
with app.app_context():
    # Create the database table if it doesn't exist
    db.create_all()

# --- Background Scheduler Setup ---
# This will run the 'fetch_and_store_data' function every 60 seconds
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(fetch_and_store_data, 'interval', seconds=60)
scheduler.start()

if __name__ == '__main__':
    # This part is for local testing and won't be used by Render
    app.run(debug=False, use_reloader=False)
