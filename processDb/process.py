from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


RESOURCES_DIR = Path(__file__).resolve().parent / "resources"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

OUTPUT_CSV_MATRIMONIOS_DEPTO_MES = OUTPUT_DIR / "matrimonios_depto_mes.csv"
OUTPUT_CSV_MATRIMONIOS_EDAD = OUTPUT_DIR / "matrimonios_edad.csv"
OUTPUT_CSV_DIVORCIOS_DEPTO_MES = OUTPUT_DIR / "divorcios_depto_mes.csv"
OUTPUT_CSV_DIVORCIOS_EDAD = OUTPUT_DIR / "divorcios_edad.csv"

SHEET_NAMES = {
    "matrimonio": {
        "edad": "Grupos de edad novio y novia",
        "depto_mes": "Mes registro y departamento",
    },
    "divorcio": {
        "edad": "Grupos edad hombre y mujer",
        "depto_mes": "Mes de registro y departamento",
    },
}

SPANISH_MONTH_TO_NUM = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

def _resolve_sheet_name(xlsx_path: Path, desired_name: str, *, keywords: List[str]) -> str:
    xls = pd.ExcelFile(xlsx_path)
    sheets = list(xls.sheet_names)

    if desired_name in sheets:
        return desired_name

    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip().lower()

    desired_norm = norm(desired_name)
    for s in sheets:
        if norm(s) == desired_norm:
            return s

    kw = [k.strip().lower() for k in keywords if k.strip()]
    for s in sheets:
        sn = norm(s)
        if all(k in sn for k in kw):
            return s

    raise ValueError(
        f"Worksheet named '{desired_name}' not found in {xlsx_path.name}. "
        f"Available sheets: {sheets}"
    )

def _drop_empty_cols(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2 = df2.dropna(axis=1, how="all")
    return df2


def _clean_cell(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return None
    return s


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = re.sub(r"\s+", " ", s).strip()
    return s2


def _norm_depto(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = _norm_text(s)
    if s2 is None:
        return None
    return s2


def _norm_age_group(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = _norm_text(s)
    if s2 is None:
        return None
    s2 = s2.replace("años", "").replace("AÑOS", "").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2_low = s2.lower()
    if s2_low in {"65 y mas", "65 y más", "65+"}:
        return "65 y más"
    if s2_low in {"menos de 15", "<15", "menor de 15"}:
        return "Menos de 15"
    if s2_low in {"menos de 20", "<20", "menor de 20"}:
        return "Menos de 20"
    if s2_low == "ignorado":
        return "Ignorado"
    if s2_low in {"todas las edades", "todas"}:
        return "Todas las edades"
    if s2_low == "total":
        return "Total"
    return s2


def _find_year_and_event_from_filename(path: Path) -> Tuple[int, str]:
    name = path.stem.lower()
    m = re.search(r"(19\d{2}|20\d{2})", name)
    if not m:
        raise ValueError(f"No year found in filename: {path.name}")
    year = int(m.group(1))

    if name.startswith("matrimonio") or name.startswith("matrimonios"):
        event = "matrimonio"
    elif name.startswith("divorcio") or name.startswith("divorcios"):
        event = "divorcio"
    else:
        raise ValueError(f"Could not infer event type from filename: {path.name}")

    return year, event


def _detect_event_from_sheets(xlsx_path: Path) -> Optional[str]:
    try:
        xls = pd.ExcelFile(xlsx_path)
        sheets_norm = [re.sub(r"\s+", " ", s).strip().lower() for s in xls.sheet_names]
    except Exception:
        return None

    if any(("novio" in s) or ("novia" in s) for s in sheets_norm):
        return "matrimonio"
    if any(("hombre" in s) or ("mujer" in s) for s in sheets_norm):
        if ("hombre" in " ".join(sheets_norm)) and ("mujer" in " ".join(sheets_norm)):
            return "divorcio"

    mat_age = re.sub(r"\s+", " ", SHEET_NAMES["matrimonio"]["edad"]).strip().lower()
    mat_depto = re.sub(r"\s+", " ", SHEET_NAMES["matrimonio"]["depto_mes"]).strip().lower()
    div_age = re.sub(r"\s+", " ", SHEET_NAMES["divorcio"]["edad"]).strip().lower()
    div_depto = re.sub(r"\s+", " ", SHEET_NAMES["divorcio"]["depto_mes"]).strip().lower()

    sset = set(sheets_norm)
    if mat_age in sset or mat_depto in sset:
        return "matrimonio"
    if div_age in sset or div_depto in sset:
        return "divorcio"

    return None

def _safe_int(x) -> Optional[int]:
    if pd.isna(x):
        return None
    try:
        return int(float(x))
    except Exception:
        s = str(x).strip()
        if s == "" or s == "-":
            return None
        m = re.search(r"\d+", s.replace(",", ""))
        return int(m.group(0)) if m else None


@dataclass(frozen=True)
class ExtractContext:
    anio: int
    evento: str


def extract_age_matrix(xlsx_path: Path, ctx: ExtractContext) -> pd.DataFrame:
    desired = SHEET_NAMES[ctx.evento]["edad"]
    if ctx.evento == "matrimonio":
        sheet = _resolve_sheet_name(xlsx_path, desired, keywords=["edad", "novio", "novia"])
    else:
        sheet = _resolve_sheet_name(xlsx_path, desired, keywords=["edad", "hombre", "mujer"])

    raw = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, dtype=object)
    raw = _drop_empty_cols(raw)

    def looks_like_age_header_row(row: pd.Series) -> bool:
        vals = [str(v).strip().lower() for v in row.tolist() if not pd.isna(v)]
        age_like = 0
        for v in vals:
            if re.search(r"\d+\s*-\s*\d+", v):
                age_like += 1
            elif "ignorado" in v or "total" == v or "menos" in v or "más" in v or "mas" in v:
                age_like += 1
        return len(vals) >= 6 and age_like >= 4

    header_row = None
    for i in range(min(len(raw), 50)):
        if looks_like_age_header_row(raw.iloc[i]):
            header_row = i
            break

    if header_row is None:
        raise ValueError(
            f"Could not find age-group header row in age matrix sheet '{sheet}' of {xlsx_path.name}"
        )

    above = raw.iloc[header_row - 1].tolist() if header_row - 1 >= 0 else [None] * raw.shape[1]
    age_header = raw.iloc[header_row].tolist()

    merged_header: List[Optional[str]] = []
    for a, b in zip(above, age_header):
        v = b if not pd.isna(b) else a
        merged_header.append(_clean_cell(v))

    col_labels = [_norm_age_group(_clean_cell(v)) for v in merged_header]

    body = raw.iloc[header_row + 1 :].copy()
    body.columns = col_labels

    first_col = body.columns[0]
    body[first_col] = body[first_col].apply(lambda x: _norm_age_group(_clean_cell(x)))
    body = body[body[first_col].notna()].copy()

    body = body.dropna(axis=1, how="all")

    woman_col = body.columns[0]

    long = body.melt(
        id_vars=[woman_col],
        var_name="edad_hombre_grupo",
        value_name="valor",
    )

    long = long.rename(columns={woman_col: "edad_mujer_grupo"})

    long["edad_mujer_grupo"] = long["edad_mujer_grupo"].apply(_norm_age_group)
    long["edad_hombre_grupo"] = long["edad_hombre_grupo"].apply(_norm_age_group)

    long = long[~long["edad_mujer_grupo"].isin({None, "Total", "Todas las edades"})]
    long = long[~long["edad_hombre_grupo"].isin({None, "Total", "Todas las edades"})]

    long["valor"] = long["valor"].apply(_safe_int)
    long = long[long["valor"].notna()].copy()
    long["valor"] = long["valor"].astype(int)

    long.insert(0, "anio", ctx.anio)
    long.insert(1, "evento", ctx.evento)
    long.insert(2, "tabla", "edad_x_edad")
    long.insert(3, "nivel_geo", "pais")
    long.insert(4, "departamento", pd.NA)
    long.insert(5, "mes", pd.NA)

    return long[
        [
            "anio",
            "evento",
            "tabla",
            "nivel_geo",
            "departamento",
            "mes",
            "edad_mujer_grupo",
            "edad_hombre_grupo",
            "valor",
        ]
    ]


def extract_depto_x_mes(xlsx_path: Path, ctx: ExtractContext) -> pd.DataFrame:
    desired = SHEET_NAMES[ctx.evento]["depto_mes"]
    sheet = _resolve_sheet_name(xlsx_path, desired, keywords=["mes", "departamento"])

    raw = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, dtype=object)
    raw = _drop_empty_cols(raw)

    def norm_cell(x) -> str:
        s = _clean_cell(x)
        return re.sub(r"\s+", " ", str(s)).strip().lower() if s is not None else ""

    MONTH_ALIASES = {
        "ene": 1, "ene.": 1, "enero": 1,
        "feb": 2, "feb.": 2, "febrero": 2,
        "mar": 3, "mar.": 3, "marzo": 3,
        "abr": 4, "abr.": 4, "abril": 4,
        "may": 5, "may.": 5, "mayo": 5,
        "jun": 6, "jun.": 6, "junio": 6,
        "jul": 7, "jul.": 7, "julio": 7,
        "ago": 8, "ago.": 8, "agosto": 8,
        "sep": 9, "sep.": 9, "sept": 9, "sept.": 9, "septiembre": 9, "setiembre": 9,
        "oct": 10, "oct.": 10, "octubre": 10,
        "nov": 11, "nov.": 11, "noviembre": 11,
        "dic": 12, "dic.": 12, "diciembre": 12,
    }

    def looks_like_month_token(t: str) -> bool:
        if not t:
            return False
        t = t.strip().lower()
        if t in MONTH_ALIASES:
            return True
        if re.fullmatch(r"0?[1-9]|1[0-2]", t):
            return True
        return False

    header_row = None
    for i in range(min(len(raw), 60)):
        row = raw.iloc[i].tolist()
        cells = [norm_cell(v) for v in row]
        if not any("departamento" in c for c in cells):
            continue
        month_like = sum(1 for c in cells if looks_like_month_token(c) or c in {"total", "tot", "total general"})
        if month_like >= 6:
            header_row = i
            break

    if header_row is None:
        raise ValueError(
            f"Could not find header row in depto×mes sheet '{sheet}' of {xlsx_path.name}. "
            "Expected a row containing 'Departamento' and month columns."
        )

    header = [ _clean_cell(v) for v in raw.iloc[header_row].tolist() ]
    header = [ _norm_text(v) for v in header ]

    body = raw.iloc[header_row + 1 :].copy()
    body.columns = header

    body[dept_col] = body[dept_col].apply(lambda x: _norm_depto(_clean_cell(x)))
    body = body[body[dept_col].notna()].copy()

    id_cols = [dept_col]
    value_cols = [c for c in body.columns if c not in id_cols]

    long = body.melt(id_vars=id_cols, value_vars=value_cols, var_name="col", value_name="valor")

    def parse_mes(col_name: Optional[str]) -> Optional[int]:
        if col_name is None:
            return None
        s = str(col_name).strip().lower()
        s = re.sub(r"\s+", " ", s)

        if s in {"total", "tot", "total general", "total general ", "total (a)"} or s.startswith("total"):
            return None

        if re.fullmatch(r"0?[1-9]|1[0-2]", s):
            return int(s)

        s2 = s.replace(".", "")
        if s2 in MONTH_ALIASES:
            return MONTH_ALIASES[s2]

        return SPANISH_MONTH_TO_NUM.get(s2)

    long["mes"] = long["col"].apply(parse_mes)

    col_norm = long["col"].apply(lambda x: re.sub(r"\s+", " ", str(x).strip().lower()) if x is not None else "")
    is_total = col_norm.eq("total") | col_norm.eq("tot") | col_norm.eq("total general") | col_norm.str.startswith("total")
    long = long[is_total | (long["mes"].notna())].copy()

    long["valor"] = long["valor"].apply(_safe_int)
    long = long[long["valor"].notna()].copy()
    long["valor"] = long["valor"].astype(int)

    def geo_level_and_depto(depto: Optional[str]) -> Tuple[str, Optional[str]]:
        if depto is None:
            return "departamento", None
        dlow = depto.lower()
        if "todos los departamentos" in dlow:
            return "pais", None
        return "departamento", depto

    levels = long[dept_col].apply(geo_level_and_depto)
    long["nivel_geo"] = [lv for lv, _ in levels]
    long["departamento"] = [dep for _, dep in levels]

    long.insert(0, "anio", ctx.anio)
    long.insert(1, "evento", ctx.evento)
    long.insert(2, "tabla", "depto_x_mes")

    long = long.rename(columns={dept_col: "_depto_orig"})

    long["edad_mujer_grupo"] = pd.NA
    long["edad_hombre_grupo"] = pd.NA

    long = long[
        [
            "anio",
            "evento",
            "tabla",
            "nivel_geo",
            "departamento",
            "mes",
            "edad_mujer_grupo",
            "edad_hombre_grupo",
            "valor",
        ]
    ]

    long["mes"] = long["mes"].astype("Int64")

    return long

def process_one_workbook(path: Path) -> pd.DataFrame:
    anio, evento_from_name = _find_year_and_event_from_filename(path)
    evento_from_sheets = _detect_event_from_sheets(path)

    evento_guess = evento_from_sheets or evento_from_name

    def sheets_exist_for(evento: str) -> bool:
        try:
            _resolve_sheet_name(path, SHEET_NAMES[evento]["edad"], keywords=["edad"])
            _resolve_sheet_name(path, SHEET_NAMES[evento]["depto_mes"], keywords=["mes", "departamento"])
            return True
        except Exception:
            return False

    evento = evento_guess
    if not sheets_exist_for(evento):
        alt = "matrimonio" if evento == "divorcio" else "divorcio"
        if sheets_exist_for(alt):
            print(
                f"WARN {path.name}: sheets do not match guessed event '{evento_guess}'. "
                f"Switching to '{alt}'."
            )
            evento = alt

    if evento_from_sheets and (evento_from_sheets != evento_from_name):
        print(
            f"WARN {path.name}: filename suggests '{evento_from_name}' but sheets look like '{evento_from_sheets}'. "
            f"Using '{evento}'."
        )

    ctx = ExtractContext(anio=anio, evento=evento)

    df_age = extract_age_matrix(path, ctx)
    df_depto = extract_depto_x_mes(path, ctx)

    parts = [p for p in (df_age, df_depto) if p is not None and not p.empty]
    if not parts:
        return pd.DataFrame()

    base_cols = [
        "anio",
        "evento",
        "tabla",
        "nivel_geo",
        "departamento",
        "mes",
        "edad_mujer_grupo",
        "edad_hombre_grupo",
        "valor",
    ]
    for i, p in enumerate(parts):
        for c in base_cols:
            if c not in p.columns:
                p[c] = pd.NA
        parts[i] = p[base_cols].copy()

    return pd.concat(parts, ignore_index=True)


def main(resources_dir: Path = RESOURCES_DIR) -> None:
    if not resources_dir.exists():
        raise FileNotFoundError(f"Resources dir not found: {resources_dir}")

    xlsx_files = sorted(resources_dir.glob("*.xlsx"))
    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files found in: {resources_dir}")

    all_parts: List[pd.DataFrame] = []

    for f in xlsx_files:
        try:
            part = process_one_workbook(f)
            all_parts.append(part)
            print(f"OK  {f.name}  -> rows: {len(part)}")
        except Exception as e:
            print(f"ERR {f.name}: {e}")
            raise

    df = pd.concat(all_parts, ignore_index=True)

    df = df.sort_values(
        by=["anio", "evento", "tabla", "nivel_geo", "departamento", "mes", "edad_mujer_grupo", "edad_hombre_grupo"],
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)

    df_matrimonios = df[df["evento"] == "matrimonio"].copy()
    df_divorcios = df[df["evento"] == "divorcio"].copy()

    df_matrimonios_depto = df_matrimonios[df_matrimonios["tabla"] == "depto_x_mes"].copy()
    df_matrimonios_edad = df_matrimonios[df_matrimonios["tabla"] == "edad_x_edad"].copy()

    df_divorcios_depto = df_divorcios[df_divorcios["tabla"] == "depto_x_mes"].copy()
    df_divorcios_edad = df_divorcios[df_divorcios["tabla"] == "edad_x_edad"].copy()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    depto_cols = ["anio", "nivel_geo", "departamento", "mes", "valor"]
    edad_cols = ["anio", "edad_mujer_grupo", "edad_hombre_grupo", "valor"]

    df_matrimonios_depto = df_matrimonios_depto[depto_cols].copy()
    df_divorcios_depto = df_divorcios_depto[depto_cols].copy()

    for d in (df_matrimonios_depto, df_divorcios_depto):
        d["mes"] = d["mes"].astype("Int64")

    df_matrimonios_edad = df_matrimonios_edad[edad_cols].copy()
    df_divorcios_edad = df_divorcios_edad[edad_cols].copy()
    df_matrimonios_depto.to_csv(OUTPUT_CSV_MATRIMONIOS_DEPTO_MES, index=False, encoding="utf-8")
    df_matrimonios_edad.to_csv(OUTPUT_CSV_MATRIMONIOS_EDAD, index=False, encoding="utf-8")

    df_divorcios_depto.to_csv(OUTPUT_CSV_DIVORCIOS_DEPTO_MES, index=False, encoding="utf-8")
    df_divorcios_edad.to_csv(OUTPUT_CSV_DIVORCIOS_EDAD, index=False, encoding="utf-8")

    print(f"\nWrote matrimonios_depto_mes file: {OUTPUT_CSV_MATRIMONIOS_DEPTO_MES}")
    print(f"Matrimonios depto_mes rows: {len(df_matrimonios_depto)} (cols: {list(df_matrimonios_depto.columns)})")
    print(f"\nWrote matrimonios_edad file: {OUTPUT_CSV_MATRIMONIOS_EDAD}")
    print(f"Matrimonios edad_x_edad rows: {len(df_matrimonios_edad)} (cols: {list(df_matrimonios_edad.columns)})")

    print(f"\nWrote divorcios_depto_mes file: {OUTPUT_CSV_DIVORCIOS_DEPTO_MES}")
    print(f"Divorcios depto_mes rows: {len(df_divorcios_depto)} (cols: {list(df_divorcios_depto.columns)})")
    print(f"\nWrote divorcios_edad file: {OUTPUT_CSV_DIVORCIOS_EDAD}")
    print(f"Divorcios edad_x_edad rows: {len(df_divorcios_edad)} (cols: {list(df_divorcios_edad.columns)})")

    print(f"\nTotal rows processed: {len(df)}")

main()