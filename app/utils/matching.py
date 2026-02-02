import re
from typing import Optional, Tuple, Dict, List
from rapidfuzz import fuzz

from app.config import (
    FUZZY_THRESHOLD_DEFAULT,
    FUZZY_THRESHOLD_MANY_CANDIDATES,
    MANY_CANDIDATES_N,
)
from app.utils.normalizers import normalizar_material, codigo_sin_ceros, normalizar_desc

def encontrar_material_en_stock(
    material_pedido: str,
    desc_pedido: str,
    materiales_stock: List[str],
    desc_stock_por_material: Dict[str, str],
) -> Tuple[Optional[str], Dict]:
    mat = normalizar_material(material_pedido)

    if mat in materiales_stock:
        return mat, {"modo": "exacto", "sim": 100, "candidatos": 1}

    code = codigo_sin_ceros(mat)
    pat = re.compile(rf"{re.escape(code)}$")
    candidatos = [m for m in materiales_stock if pat.search(m)]

    if not candidatos:
        pat2 = re.compile(rf"{re.escape(code)}")
        candidatos = [m for m in materiales_stock if pat2.search(m)]

    if not candidatos:
        return None, {"modo": "sin_match", "sim": 0, "candidatos": 0}

    dp = normalizar_desc(desc_pedido)
    if not dp:
        candidatos.sort(key=lambda x: (not x.endswith(code), len(x)))
        return candidatos[0], {"modo": "regex_sin_desc_pedido", "sim": None, "candidatos": len(candidatos)}

    scored = []
    for m in candidatos:
        ds = normalizar_desc(desc_stock_por_material.get(m, ""))
        sim = fuzz.token_set_ratio(dp, ds) if ds else 0
        scored.append((sim, m))

    scored.sort(reverse=True, key=lambda x: x[0])
    best_sim, best_mat = scored[0]

    threshold = FUZZY_THRESHOLD_DEFAULT
    if len(candidatos) >= MANY_CANDIDATES_N:
        threshold = FUZZY_THRESHOLD_MANY_CANDIDATES

    if best_sim >= threshold:
        return best_mat, {"modo": "regex+fuzzy", "sim": best_sim, "candidatos": len(candidatos)}
    return None, {"modo": "regex_fuzzy_bajo", "sim": best_sim, "candidatos": len(candidatos)}