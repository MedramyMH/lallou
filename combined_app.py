import threading
import subprocess

from flask import Flask
import os

def run_app():
    subprocess.run(["python", "app.py"])

def run_bot():
    subprocess.run(["python", "main.py"])



def fake_web():
    app = Flask(__name__)

    @app.route('/')
    def home():
        return "App is running"

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    t1 = threading.Thread(target=run_app)
    t2 = threading.Thread(target=run_bot)
    t3 = threading.Thread(target=fake_web)
    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()
