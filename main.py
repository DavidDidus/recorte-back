from io import BytesIO
import re
from typing import Optional, Tuple, Dict, List

import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from rapidfuzz import fuzz

# =========================
# CONFIGURACIÓN
# =========================
CENTRO_BODEGA = 2306
CENTRO_LOGISTICO = 2307

DEFAULT_HOJA_PEDIDOS = "Hoja1"
DEFAULT_HOJA_STOCK = "Data"

# Umbrales para aceptar match por similitud
FUZZY_THRESHOLD_DEFAULT = 80
FUZZY_THRESHOLD_MANY_CANDIDATES = 88   # si hay muchos candidatos, exigir más
MANY_CANDIDATES_N = 5

app = FastAPI(title="Validación Stock API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# ENDPOINT: LISTAR HOJAS
# =========================
@app.post("/api/sheets")
async def listar_hojas(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Sube un archivo Excel (.xlsx o .xls).")

    content = await file.read()
    try:
        xls = pd.ExcelFile(BytesIO(content))
        return {"ok": True, "sheets": xls.sheet_names}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude leer el Excel: {str(e)}")


# =========================
# UTILIDADES GENERALES
# =========================

def clasificar_centro(centro: int) -> str:
    centro_str = str(centro).strip()
    if centro_str.endswith("6"):
        return "bodega_principal"
    elif centro_str.endswith("7"):
        return "bodega_externa"
    return "otros"

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
    # deja letras, números, espacios y algunos separadores comunes
    s = re.sub(r"[^a-z0-9áéíóúüñ\s\-\/]", "", s)
    return s


# =========================
# PREPARACIÓN DE PEDIDOS
# =========================
def preparar_pedidos(df_pedidos: pd.DataFrame) -> pd.DataFrame:
    df = limpiar_columnas(df_pedidos)

    col_prod = encontrar_columna(
        df,
        candidatos_exactos=("Producto",),
        contiene=("producto", "material", "sku", "cod"),
    )
    col_cnt = encontrar_columna(
        df,
        candidatos_exactos=("Cnt.Pedidos", "Cnt Pedidos", "Cnt.Pedido", "Cantidad"),
        contiene=("cnt", "pedido", "pedidos", "cantidad"),
    )
    col_desc = encontrar_columna(
        df,
        candidatos_exactos=("Desc.Reducida", "Desc Reducida", "Descripción", "Descripcion"),
        contiene=("desc", "descrip", "nombre"),
    )

    if not col_prod or not col_cnt:
        raise KeyError(f"Pedidos: no encontré columnas. Columnas detectadas: {list(df.columns)}")

    cols = [col_prod, col_cnt] + ([col_desc] if col_desc else [])
    df = df[cols].copy()

    rename_map = {col_prod: "Producto", col_cnt: "Cnt.Pedidos"}
    if col_desc:
        rename_map[col_desc] = "NombreProducto"
    df = df.rename(columns=rename_map)

    df["Producto"] = df["Producto"].apply(normalizar_material)
    df["Cnt.Pedidos"] = pd.to_numeric(df["Cnt.Pedidos"], errors="coerce").fillna(0).astype(int)

    if "NombreProducto" not in df.columns:
        df["NombreProducto"] = ""

    def first_non_empty(s: pd.Series) -> str:
        s = s.astype(str).str.strip()
        s = s[s != ""]
        return s.iloc[0] if len(s) else ""

    out = (
        df.groupby("Producto", as_index=False)
          .agg({"Cnt.Pedidos": "sum", "NombreProducto": first_non_empty})
    )
    return out


# =========================
# PREPARACIÓN DE STOCK (incluye Texto breve material)
# =========================
def preparar_stock(df_stock: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    df = limpiar_columnas(df_stock)

    col_mat = encontrar_columna(df, candidatos_exactos=("Material",), contiene=("material", "producto", "sku", "cod"))
    col_cen = encontrar_columna(df, candidatos_exactos=("Centro",), contiene=("centro", "werks"))
    col_lib = encontrar_columna(
        df,
        candidatos_exactos=("Libre utilización", "Libre utilizacion", "Libre utilización ", "Libre utilizacion "),
        contiene=("libre", "utiliz", "dispon", "available"),
    )
    col_desc_stock = encontrar_columna(
        df,
        candidatos_exactos=("Texto breve material", "Texto breve de material", "Texto breve"),
        contiene=("texto breve", "breve material", "descripcion", "descripción"),
    )

    if not col_mat or not col_cen or not col_lib:
        raise KeyError(f"Stock: no encontré columnas. Columnas detectadas: {list(df.columns)}")

    cols = [col_mat, col_cen, col_lib] + ([col_desc_stock] if col_desc_stock else [])
    df = df[cols].copy()

    rename_map = {col_mat: "Material", col_cen: "Centro", col_lib: "Libre_utilizacion"}
    if col_desc_stock:
        rename_map[col_desc_stock] = "DescStock"
    df = df.rename(columns=rename_map)

    df["Material"] = df["Material"].astype(str).str.strip()
    df["Centro"] = pd.to_numeric(df["Centro"], errors="coerce").astype("Int64")
    df["Libre_utilizacion"] = pd.to_numeric(df["Libre_utilizacion"], errors="coerce").fillna(0)

    # Mapa material -> descripción (una por material)
    desc_map = {}
    if "DescStock" in df.columns:
        tmp = df[["Material", "DescStock"]].copy()
        tmp["DescStock"] = tmp["DescStock"].astype(str).str.strip()
        # primera no vacía
        tmp = tmp[tmp["DescStock"] != ""]
        for m, g in tmp.groupby("Material"):
            desc_map[m] = g["DescStock"].iloc[0]

    # Agrupar stock por material/centro
    df_stock_agg = df.groupby(["Material", "Centro"], as_index=False)["Libre_utilizacion"].sum()

    return df_stock_agg, desc_map


# =========================
# MATCH FLEXIBLE: exacto -> regex -> fuzzy con desc
# =========================
def encontrar_material_en_stock(
    material_pedido: str,
    desc_pedido: str,
    materiales_stock: List[str],
    desc_stock_por_material: Dict[str, str],
) -> Tuple[Optional[str], Dict]:
    mat = normalizar_material(material_pedido)

    # 1) exacto
    if mat in materiales_stock:
        return mat, {"modo": "exacto", "sim": 100, "candidatos": 1}

    # 2) regex: contiene / termina en código sin ceros
    code = codigo_sin_ceros(mat)
    # Preferimos terminar en code (mucha gente tiene ceros a la izquierda)
    # y además permitimos contenerlo para casos raros.
    pat = re.compile(rf"{re.escape(code)}$")
    candidatos = [m for m in materiales_stock if pat.search(m)]

    if not candidatos:
        pat2 = re.compile(rf"{re.escape(code)}")
        candidatos = [m for m in materiales_stock if pat2.search(m)]

    if not candidatos:
        return None, {"modo": "sin_match", "sim": 0, "candidatos": 0}

    # 3) fuzzy por descripción (Texto breve material)
    dp = normalizar_desc(desc_pedido)
    # si no hay descripción en pedido, no podemos validar fuzzy => tomar mejor heurística
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


# =========================
# CÁLCULOS
# =========================
def obtener_stock_por_tipo(df_stock: pd.DataFrame, material: str) -> dict:
    mat = str(material).strip()
    filas = df_stock[df_stock["Material"] == mat]

    if filas.empty:
        return {
            "bodega_principal": 0,
            "bodega_externa": 0,
            "otros": 0,
            "detalle_centros": {},
        }

    # Cast keys/values to JSON-safe Python types
    detalle = {}
    for centro, stock in filas.groupby("Centro")["Libre_utilizacion"].sum().items():
        if pd.isna(centro):
            continue
        detalle[str(int(centro))] = float(stock)

    stock_principal = 0.0
    stock_externa = 0.0
    stock_otros = 0.0

    for centro, stock in detalle.items():
        tipo = clasificar_centro(centro)
        if tipo == "bodega_principal":
            stock_principal += stock
        elif tipo == "bodega_externa":
            stock_externa += stock
        else:
            stock_otros += stock

    return {
        "bodega_principal": float(stock_principal),
        "bodega_externa": float(stock_externa),
        "otros": float(stock_otros),
        "detalle_centros": detalle,
    }

def evaluar_producto_por_tipo(material, nombre, pedidos, stock_tipos, existe_material):
    pedidos = int(pedidos)
    sp = float(stock_tipos["bodega_principal"])
    se = float(stock_tipos["bodega_externa"])

    asigna_principal = min(pedidos, sp)
    restante = pedidos - asigna_principal
    asigna_externa = min(restante, se)
    faltante = pedidos - asigna_principal - asigna_externa

    # ============================
    # DEFINICIÓN DE ESTADO
    # ============================
    if pedidos == 0:
        estado = "Sin demanda"
    elif not existe_material:
        estado = "AVISO - Producto no existe en stock"
    elif faltante == 0 and asigna_externa > 0:
        # Aquí agregamos el número de cajas desde bodega externa
        estado = f"OK - Completa con bodega externa ({asigna_externa} cajas)"
    elif asigna_principal == pedidos:
        estado = "OK - Stock completo en bodega principal"
    elif sp + se == 0:
        estado = "NO - Sin stock"
    else:
        estado = "NO - Stock insuficiente"

    return {
        "Producto": normalizar_material(material),
        "NombreProducto": str(nombre),
        "Pedidos": pedidos,
        "Stock_Bodega_Principal": sp,
        "Stock_Bodega_Externa": se,
        "Stock_Otros": float(stock_tipos["otros"]),
        "Asignado_Principal": asigna_principal,
        "Asignado_Externa": asigna_externa,
        "Faltante": faltante,
        "DetalleCentros": stock_tipos["detalle_centros"],
        "Estado": estado,
    }

# =========================
# ENDPOINT: VALIDAR STOCK
# =========================
@app.post("/api/validar-stock")
async def validar_stock(
    file: UploadFile = File(...),
    hoja_pedidos: str = DEFAULT_HOJA_PEDIDOS,
    hoja_stock: str = DEFAULT_HOJA_STOCK,
):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Sube un archivo Excel (.xlsx o .xls).")

    content = await file.read()
    try:
        xls = pd.ExcelFile(BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude leer el Excel: {str(e)}")

    sheets = xls.sheet_names
    if hoja_pedidos not in sheets:
        raise HTTPException(status_code=400, detail=f"No existe la hoja pedidos '{hoja_pedidos}'. Hojas: {sheets}")
    if hoja_stock not in sheets:
        raise HTTPException(status_code=400, detail=f"No existe la hoja stock '{hoja_stock}'. Hojas: {sheets}")

    try:
        df_pedidos_raw = pd.read_excel(xls, sheet_name=hoja_pedidos)
        df_stock_raw = pd.read_excel(xls, sheet_name=hoja_stock)

        df_pedidos = preparar_pedidos(df_pedidos_raw)
        df_stock, desc_stock_map = preparar_stock(df_stock_raw)

        materiales_stock = df_stock["Material"].astype(str).unique().tolist()

        resultados = []
        for _, row in df_pedidos.iterrows():
            material_pedido = row["Producto"]
            pedidos = int(row["Cnt.Pedidos"])
            nombre = row.get("NombreProducto", "")

            # MATCH: exacto -> regex -> fuzzy con Texto breve material
            material_match, info = encontrar_material_en_stock(
                material_pedido=material_pedido,
                desc_pedido=nombre,
                materiales_stock=materiales_stock,
                desc_stock_por_material=desc_stock_map,
            )

            if material_match is None:
                # No match confiable: se avisa
                resultados.append({
                    "Producto": normalizar_material(material_pedido),
                    "NombreProducto": str(nombre),
                    "Pedidos": int(pedidos),
                    "Stock_2306": 0,
                    "Stock_2307": 0,
                    "Asigna_2306": 0,
                    "Asigna_2307": 0,
                    "Faltante": int(pedidos),
                    "Estado": "AVISO - No se encontró match confiable en stock",
                    # Si quieres auditar, descomenta:
                    # "DebugMatch": info,
                })
                continue

            stock_tipos = obtener_stock_por_tipo(df_stock, material_match)

            resultados.append(
                evaluar_producto_por_tipo(
                    material=material_pedido,
                    nombre=nombre,
                    pedidos=pedidos,
                    stock_tipos=stock_tipos,
                    existe_material=True
                )
            )

        return {
            "ok": True,
            "sheets": sheets,
            "hoja_pedidos": hoja_pedidos,
            "hoja_stock": hoja_stock,
            "resultados": resultados,
        }

    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando: {str(e)}")
