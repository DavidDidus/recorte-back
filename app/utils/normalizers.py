import re

def normalizar_material(material) -> str:
    s = str(material).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s

def codigo_sin_ceros(material: str) -> str:
    s = str(material).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s2 = s.lstrip("0")
    return s2 if s2 else s

def normalizar_desc(texto: str) -> str:
    if texto is None:
        return ""
    s = str(texto).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9áéíóúüñ\s\-\/]", "", s)
    return s

def clasificar_centro(centro: int) -> str:
    centro_str = str(centro).strip()
    if centro_str.endswith("6"):
        return "bodega_principal"
    elif centro_str.endswith("7"):
        return "bodega_externa"
    return "otros"