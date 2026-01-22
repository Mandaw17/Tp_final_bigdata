import os
import re
import requests

def download_public_drive_zips(folder_url: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    response = requests.get(folder_url)
    response.raise_for_status()

    html = response.text

    
    file_ids = set(re.findall(r'"([a-zA-Z0-9_-]{25,})"', html))

    for file_id in file_ids:
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        r = requests.get(download_url, stream=True)

        content_type = r.headers.get("Content-Type", "")
        if "zip" not in content_type:
            continue  

        filename = f"{file_id}.zip"
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f"Downloaded: {filename}")
