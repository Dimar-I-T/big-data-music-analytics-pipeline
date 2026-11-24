import json
import random
import time
import uuid
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9094"
TOPIC_NAME = "music-play-logs"

def fetch_real_tracks():
    """Mengambil metadata lagu asli dari iTunes Search API sebagai katalog dasar."""
    keywords = ["pop", "rock", "indie", "jazz", "hiphop", "indonesia"]
    catalog = []
    print("Mengambil katalog lagu dari API publik...")
    
    for kw in keywords:
        url = f"https://itunes.apple.com/search?term={kw}&entity=song&limit=25"
        try:
            resp = requests.get(url, timeout=10).json()
            for item in resp.get("results", []):
                if item.get("trackId") and item.get("trackTimeMillis"):
                    catalog.append({
                        "track_id": f"trk_{item['trackId']}",
                        "track_name": item.get("trackName", "Unknown"),
                        "artist": item.get("artistName", "Unknown"),
                        "genre": item.get("primaryGenreName", "Pop"),
                        "duration_ms": int(item["trackTimeMillis"]),
                        "audio_features": {
                            "energy": round(random.uniform(0.35, 0.95), 2),
                            "danceability": round(random.uniform(0.40, 0.92), 2),
                            "tempo": round(random.uniform(75.0, 165.0), 1)
                        }
                    })
        except Exception as e:
            print(f"Gagal mengambil keyword {kw}: {e}")
            
    print(f"Berhasil memuat {len(catalog)} lagu unik ke katalog.")
    return catalog

def main():
    catalog = fetch_real_tracks()
    if not catalog:
        print("Katalog kosong, periksa koneksi internet.")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    platforms = ["spotify", "apple_music"]
    regions = ["ID", "SG", "MY", "US", "JP"]

    print(f"Mulai mengirim aliran log pemutaran ke topic '{TOPIC_NAME}'... (Tekan Ctrl+C untuk berhenti)")
    event_count = 0

    try:
        while True:
            track = random.choice(catalog)
            platform = random.choice(platforms)
            duration = track["duration_ms"]

            if random.random() < 0.25:
                played_ms = random.randint(5000, min(29000, duration))
            else:
                played_ms = random.randint(int(duration * 0.5), duration)

            event = {
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "platform": platform,
                "user_region": random.choice(regions),
                "track_id": track["track_id"],
                "track_name": track["track_name"],
                "artist": track["artist"],
                "genre": track["genre"],
                "duration_ms": duration,
                "played_ms": played_ms,
                "audio_features": track["audio_features"]
            }

            producer.send(TOPIC_NAME, key=platform, value=event)
            event_count += 1

            if event_count % 50 == 0:
                producer.flush()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Terkirim {event_count} events ke Kafka")

            time.sleep(0.05)  
    except KeyboardInterrupt:
        print("\nMenghentikan producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()