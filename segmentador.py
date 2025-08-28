import pandas as pd
from io import BytesIO
from typing import Optional

status_to_dictamen = {
    "CONTACTO EFECTIVO": [
        "LLAMAR DESPUES", "MENSAJE CON FAMILIAR", "MENSAJE CON TERCERO",
        "MENSAJE CON TERCEROS", "PAGO EFECTUADO", "PAGO PARCIAL"
    ],
    "CONTACTO NO EFECTIVO": [
        "ACLARACION", "CLIENTE NO DEFINE", "CUENTA CORRIENTE",
        "CUENTA LIQUIDADA", "DEFUNCION", "NEGATIVA DE PAGO"
    ],
    "ILOCALIZABLE": [
        "FUERA DE SERVICIO", "ILOCALIZABLE", "TELEFONO EQUIVOCADO"
    ],
    "PROMESA DE PAGO": [
        "PROMESA DE PAGO", "SEGUIMIENTO A PP", "SEGUIMIENTO A PROMESA"
    ],
    "SIN CONTACTO": [
        "BUZON DE VOZ", "CUELGA LLAMADA", "NO CONTESTA"
    ],
    None: [None, ""],
}

def _null_mask(s: pd.Series) -> pd.Series:
    return s.isna() | (s == "")

def segmentar(
    df: pd.DataFrame,
    estatus: Optional[str] = None,
    dictamen: Optional[str] = None,
    campana: Optional[str] = None,
) -> pd.DataFrame:
    keep = pd.Series(True, index=df.index)

    if estatus is not None:                        
        col = df["estatus_resultado_mejor_dictamen_historico"]
        keep &= _null_mask(col) if estatus == "None" else (col == estatus)

    if dictamen is not None:
        col = df["mejor_dictamen_historico"]
        keep &= _null_mask(col) if dictamen == "None" else (col == dictamen)

    if campana:                                    
        keep &= df["campaña"] == campana

    return df.loc[keep].copy()