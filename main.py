import shutil
from pathlib import Path
from il_supermarket_scarper import ScarpingTask

STORE_ID = "013"
DUMP_DIR = Path("dumps/RamiLevy")
OUTPUT_DIR = Path(f"dumps/RamiLevy_store_{STORE_ID}")

# Download all file types from Rami Levy
scraper = ScarpingTask(
    enabled_scrapers=["RAMI_LEVY"],
    multiprocessing=1,
)
thread = scraper.start()
thread.join()
print("Done downloading Rami Levy data.")

# Copy only store 013 files (גבעת שאול בית הדפוס)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
count = 0
for f in DUMP_DIR.glob("*"):
    if f"-{STORE_ID}-" in f.name or f"-{STORE_ID}." in f.name:
        shutil.copy2(f, OUTPUT_DIR / f.name)
        count += 1

# Also copy the store file
for f in DUMP_DIR.glob("Stores*"):
    shutil.copy2(f, OUTPUT_DIR / f.name)

print(f"Copied {count} files for store {STORE_ID} to {OUTPUT_DIR}")