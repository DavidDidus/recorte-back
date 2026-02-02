import pandas as pd

def limpiar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", " ", regex=False)
        .str.strip()
    )
    return df

def encontrar_columna(df: pd.DataFrame, candidatos_exactos=(), contiene=()):
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}

    for c in candidatos_exactos:
        if c.lower() in lower_map:
            return lower_map[c.lower()]

    for c in cols:
        cl = c.lower()
        for token in contiene:
            if token.lower() in cl:
                return c
    return None