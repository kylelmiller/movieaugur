"""Web Service Gateway Interface for Gunicorn"""
from popularityservice.entrypoints.flask_app import app

if __name__ == "__main__":
    app.run()
