import threading
import subprocess

def run_app():
    subprocess.run(["python", "app.py"])

def run_bot():
    subprocess.run(["python", "main.py"])

if __name__ == "__main__":
    t1 = threading.Thread(target=run_app)
    t2 = threading.Thread(target=run_bot)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
