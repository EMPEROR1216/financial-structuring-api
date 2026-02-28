import pandas as pd

from app.config import debug_log


def create_csv(data, path):
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
    debug_log("CSV", f"Generated {path}: {len(df)} rows, columns: {list(df.columns)}")