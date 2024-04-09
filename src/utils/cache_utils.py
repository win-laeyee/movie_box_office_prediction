import os
import pickle
import datetime
import logging

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def is_cache_fresh(cache_file):
    if os.path.exists(cache_file):
        modified_time = os.path.getmtime(cache_file)
        current_time = datetime.datetime.now().timestamp()
        return current_time - modified_time < CACHE_EXPIRATION_TIME
    return False

def load_from_cache(cache_file):
    with open(cache_file, "rb") as f:
        return pickle.load(f)

def save_to_cache(data, cache_file):
    with open(cache_file, "wb") as f:
        pickle.dump(data, f)

def query_or_load_from_cache(query_function, table_name):
    cache_file = os.path.join(CACHE_DIR, f"{table_name}.pkl")
    if is_cache_fresh(cache_file):
        logging.info("Data loaded from cache")
        return load_from_cache(cache_file)
    else:
        data = query_function()
        logging.info("Data queried from Big Query")
        save_to_cache(data, cache_file)
        logging.info("Data saved to cache for querying next time")

        return data

def clear_cache():
    for filename in os.listdir(CACHE_DIR):
        file_path = os.path.join(CACHE_DIR, filename)
        try:
            if os.path.isfile(file_path):
                logging.info("Cleared cache")
                os.unlink(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

CACHE_EXPIRATION_TIME = 3600  # 1 hour
clear_cache()
