import json
import subprocess
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(current_dir, 'credential.json')
with open(CREDENTIALS_FILE) as f:
    credentials = json.load(f)

for conn_id, conn_info in credentials.items():
    # Bangun dict sesuai format yang diterima oleh --conn-json
    conn_json = {
        "conn_type": conn_info.get("conn_type", ""),
        "login": conn_info.get("login", ""),
        "password": conn_info.get("password", ""),
        "host": conn_info.get("host", ""),
        "port": conn_info.get("port", ""),
        "schema": conn_info.get("schema", ""),
        "extra": conn_info.get("extra", {})
    }
    if "extra" in conn_info:
        conn_json["extra"] = conn_info["extra"]
    # Buat perintah untuk menambahkan koneksi ke Airflow
    result = subprocess.run([
        "airflow", "connections", "add", conn_id, "--conn-json", json.dumps(conn_json)
    ])


