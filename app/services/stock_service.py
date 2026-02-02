from typing import Tuple, Dict
import pandas as pd

from app.utils.excel import limpiar_columnas, encontrar_columna
from app.utils.normalizers import normalizar_material, clasificar_centro
from app.utils.matching import encontrar_material_en_stock

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

    desc_map = {}
    if "DescStock" in df.columns:
        tmp = df[["Material", "DescStock"]].copy()
        tmp["DescStock"] = tmp["DescStock"].astype(str).str.strip()
        tmp = tmp[tmp["DescStock"] != ""]
        for m, g in tmp.groupby("Material"):
            desc_map[m] = g["DescStock"].iloc[0]

    df_stock_agg = df.groupby(["Material", "Centro"], as_index=False)["Libre_utilizacion"].sum()

    return df_stock_agg, desc_map

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

    if pedidos == 0:
        estado = "Sin demanda"
    elif not existe_material:
        estado = "AVISO - Producto no existe en stock"
    elif faltante == 0 and asigna_externa > 0:
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

def procesar_validacion(df_pedidos_raw: pd.DataFrame, df_stock_raw: pd.DataFrame):
    df_pedidos = preparar_pedidos(df_pedidos_raw)
    df_stock, desc_stock_map = preparar_stock(df_stock_raw)

    materiales_stock = df_stock["Material"].astype(str).unique().tolist()

    resultados = []
    for _, row in df_pedidos.iterrows():
        material_pedido = row["Producto"]
        pedidos = int(row["Cnt.Pedidos"])
        nombre = row.get("NombreProducto", "")

        material_match, _info = encontrar_material_en_stock(
            material_pedido=material_pedido,
            desc_pedido=nombre,
            materiales_stock=materiales_stock,
            desc_stock_por_material=desc_stock_map,
        )

        if material_match is None:
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

    return resultados, df_pedidos, df_stock