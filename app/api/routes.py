from io import BytesIO

import pandas as pd
from fastapi import APIRouter, File, UploadFile, HTTPException

from app.config import DEFAULT_HOJA_PEDIDOS, DEFAULT_HOJA_STOCK
from app.services.stock_service import procesar_validacion

router = APIRouter()

@router.post("/api/sheets")
async def listar_hojas(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Sube un archivo Excel (.xlsx o .xls).")

    content = await file.read()
    try:
        xls = pd.ExcelFile(BytesIO(content))
        return {"ok": True, "sheets": xls.sheet_names}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude leer el Excel: {str(e)}")

@router.post("/api/validar-stock")
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

        resultados, _df_pedidos, _df_stock = procesar_validacion(df_pedidos_raw, df_stock_raw)

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