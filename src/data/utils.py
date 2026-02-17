from pathlib import Path

# functions
def setup_dirs():
    """
    Creates directory structure
    """
    dirs = ["data/raw/", "data/processed"]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)