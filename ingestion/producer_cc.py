"""
Círculo de Crédito XML Producer
================================
Generates synthetic XML files replicating the structure of a Círculo de Crédito
bureau report (SIC - Sociedad de Información Crediticia).

For each XML generated, one application record is created with:
  - uuid          : unique application identifier (UUID v4)
  - date          : random date within --fecha-inicio / --fecha-fin (uniform distribution)
  - amount        : disbursement amount sampled from a Pareto/Power-law distribution (80/20 effect)
  - curp          : CURP of the first persona in the associated XML

Application records are accumulated and exported as a JSON file (applications.json).
XMLs can be bundled into a ZIP for easier handling and GCS upload.

Usage:
    python producer_cc.py                                              # 1 XML, no extras
    python producer_cc.py --cantidad 5                                 # 5 personas per XML
    python producer_cc.py --archivos 10                                # 10 XMLs
    python producer_cc.py --destino ./output --archivos 10             # custom output folder
    python producer_cc.py --archivos 10 --applications app.json        # generate application JSON
    python producer_cc.py --archivos 10 --zip xmls.zip                 # bundle XMLs into ZIP
    python producer_cc.py --archivos 10 --fecha-inicio 2023-01-01 --fecha-fin 2023-12-31

Dependencies:
    pip install faker
"""

import random
import uuid
import zipfile
import argparse
from datetime import date, timedelta, datetime
from xml.dom import minidom
import xml.etree.ElementTree as ET
from faker import Faker
import hashlib

try:
    from google.cloud import storage as gcs
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

fake = Faker("es_MX")

# ─────────────────────────── CATÁLOGOS ────────────────────────────────────────

ESTADOS_MX = [
    "AGS", "BC", "BCS", "CAMP", "CHIS", "CHIH", "CDMX", "COAH",
    "COL", "DGO", "GTO", "GRO", "HGO", "JAL", "MEX", "MICH",
    "MOR", "NAY", "NL", "OAX", "PUE", "QRO", "QROO", "SLP",
    "SIN", "SON", "TAB", "TAMPS", "TLAX", "VER", "YUC", "ZAC",
]

# Tipo de crédito (TipoCredito)
TIPO_CREDITO = {
    "PP": "Préstamo Personal",
    "CF": "Crédito de Fomento / Gobierno",
    "TC": "Tarjeta de Crédito",
    "CP": "Crédito de Proveedores / Comercial",
    "HI": "Hipotecario",
    "AU": "Automotriz",
    "NM": "Nómina",
    "AB": "Arrendamiento / Bienes",
    "PE": "Préstamo Empresarial",
}

# Tipo de cuenta (TipoCuenta)
TIPO_CUENTA = {
    "F": "Fijo",       # Monto y pagos definidos
    "R": "Revolvente", # Línea de crédito reutilizable
    "L": "Línea de crédito",
}

# Tipo de responsabilidad (TipoResponsabilidad)
TIPO_RESPONSABILIDAD = {
    "I": "Individual",
    "M": "Mancomunado",
    "O": "Obligado Solidario / Aval",
}

# Frecuencia de pagos (FrecuenciaPagos)
FRECUENCIA_PAGOS = {
    "M": "Mensual",
    "C": "Catorcenal",
    "Q": "Quincenal",
    "S": "Semanal",
    "U": "Única vez",
    "A": "Anual",
}

# Estado civil
ESTADO_CIVIL = ["S", "C", "D", "V", "U"]  # Soltero, Casado, Divorciado, Viudo, Unión libre

# Tipo domicilio
TIPO_DOMICILIO = ["C", "P", "N"]  # Conocido, Previo, Nuevo

# Clave de prevención
CLAVE_PREVENCION = ["CC", "IM", "CA", "CI", ""]  # Cuenta cancelada, impugnado, etc.

# Códigos de razón FICO (motivos del score)
RAZONES_FICO = [
    "D2", "K0", "P9", "G1", "A3", "B1", "C2", "E5", "F4",
    "H7", "J2", "L3", "M1", "N8", "O2", "Q1", "R5", "S3",
]

# Nombres de otorgantes de crédito representativos
# ── Banca Múltiple (fuente: ABM / CNBV 2024-2025) ─────────────────────────────
BANCOS_BANCA_MULTIPLE = [
    # G8 — bancos sistémicamente importantes
    "BBVA MEXICO",
    "BANCO SANTANDER MEXICO",
    "BANCO MERCANTIL DEL NORTE (BANORTE)",
    "BANCO NACIONAL DE MEXICO (BANAMEX)",
    "HSBC MEXICO",
    "SCOTIABANK MEXICO",
    "BANCO INBURSA",
    "CITI MEXICO",
    # Banca mediana y especializada
    "BANCO DEL BAJIO",
    "BANCO AZTECA",
    "BANCA AFIRME",
    "BANCOPPEL",
    "BANCO REGIONAL DE MONTERREY (BANREGIO)",
    "HEY BANCO",
    "BANCO MONEX",
    "BANCO MULTIVA",
    "BANCO INVEX",
    "BANCA MIFEL",
    "BANCO VE POR MAS",
    "INTERCAM BANCO",
    "BANSI",
    "BANCO BASE",
    "BANCO AUTOFIN MEXICO",
    "CONSUBANCO",
    "BANCO INMOBILIARIO MEXICANO",
    "FUNDACION DONDE BANCO",
    "BANCO PAGATODO",
    "KAPITAL BANK",
    "BANCO SABADELL MEXICO",
    # Banca de nicho / especializada
    "COMPARTAMOS BANCO",
    "BANCO ACTINVER",
    "BANCO JP MORGAN MEXICO",
    "BANK OF AMERICA MEXICO",
    "BARCLAYS BANK MEXICO",
    "BNP PARIBAS MEXICO",
    "VOLKSWAGEN BANK MEXICO",
    "BANCO KEB HANA MEXICO",
    "BANCO SHINHAN DE MEXICO",
    "MUFG BANK MEXICO",
    "MIZUHO BANK MEXICO",
    "INDUSTRIAL AND COMMERCIAL BANK OF CHINA MEXICO",
    "BANK OF CHINA MEXICO",
    "BANCO S3 CACEIS MEXICO",
    # Neobancos con licencia bancaria
    "NU MEXICO",
    "OPENBANK MEXICO",
    "REVOLUT MEXICO",
    "UALA MEXICO",
    "BANKAOOL",
    # Banca de desarrollo (gobierno)
    "NACIONAL FINANCIERA (NAFINSA)",
    "BANCO NACIONAL DE COMERCIO EXTERIOR (BANCOMEXT)",
    "BANCO DEL EJERCITO (BANJERCITO)",
    "BANCO NACIONAL DE OBRAS (BANOBRAS)",
    "BANCO DEL BIENESTAR",
    "FINANCIERA NACIONAL DE DESARROLLO (FND)",
    # SOFOMEs y microfinancieras reguladas que aparecen en buró
    "COMPARTAMOS FINANCIERA",
    "CREDITO REAL",
    "CONFICRÉDITO",
    "FINSOL",
    "CAME MICROFINANCIERA",
    "FINANCIERA INDEPENDENCIA",
    "APOYO ECONOMICO FAMILIAR",
    "CRÉDITO FAMILIAR",
    "TE CREEMOS",
    "SOFOM EXPRESS",
    "CAJA POP MEXICANA",
    "CAJA SOLIDARIA",
    "FINANCIERA RURAL",
    "SOLUCION ASEA",
]

# Lista de otorgantes usada en la generación (alias para compatibilidad)
OTORGANTES = BANCOS_BANCA_MULTIPLE

COLONIAS = [
    "CENTRO", "MILENIO II", "LAS FLORES", "SAN JUAN", "VISTA HERMOSA",
    "INDEPENDENCIA", "REVOLUCION", "LOMAS DEL VALLE", "JARDINES DEL BOSQUE",
    "NUEVA ESPAÑA", "EMILIANO ZAPATA", "BENITO JUAREZ", "INSURGENTES",
    "SANTA FE", "DOCTORES", "NARVARTE", "POLANCO",
]

# One office per state — 4 municipalities per state.
# office_id is derived as  STATE_MUNICIPALITY  (e.g. "AGS_AGUASCALIENTES").
# This dict is the single source of truth used by both:
#   - generar_domicilios()  — to pick a realistic municipality
#   - generar_offices_csv() — to produce the offices reference CSV
OFFICES_BY_STATE: dict[str, list[str]] = {
    "AGS":  ["AGUASCALIENTES", "CALVILLO", "RINCON DE ROMOS", "PABELLON DE ARTEAGA"],
    "BC":   ["TIJUANA", "MEXICALI", "ENSENADA", "TECATE"],
    "BCS":  ["LA PAZ", "LOS CABOS", "COMONDÚ", "LORETO"],
    "CAMP": ["CAMPECHE", "CIUDAD DEL CARMEN", "CHAMPOTON", "ESCARCEGA"],
    "CHIS": ["TUXTLA GUTIERREZ", "SAN CRISTOBAL DE LAS CASAS", "TAPACHULA", "COMITAN"],
    "CHIH": ["CHIHUAHUA", "CIUDAD JUAREZ", "DELICIAS", "PARRAL"],
    "CDMX": ["CUAUHTEMOC", "IZTAPALAPA", "COYOACAN", "TLALPAN"],
    "COAH": ["SALTILLO", "TORREON", "MONCLOVA", "PIEDRAS NEGRAS"],
    "COL":  ["COLIMA", "MANZANILLO", "TECOMÁN", "VILLA DE ALVAREZ"],
    "DGO":  ["DURANGO", "GOMEZ PALACIO", "LERDO", "SANTIAGO PAPASQUIARO"],
    "GTO":  ["LEON", "IRAPUATO", "CELAYA", "SALAMANCA"],
    "GRO":  ["CHILPANCINGO", "ACAPULCO", "ZIHUATANEJO", "IGUALA"],
    "HGO":  ["PACHUCA", "TULANCINGO", "TULA DE ALLENDE", "IXMIQUILPAN"],
    "JAL":  ["GUADALAJARA", "ZAPOPAN", "TLAQUEPAQUE", "TONALA"],
    "MEX":  ["TOLUCA", "ECATEPEC", "NAUCALPAN", "NEZAHUALCOYOTL"],
    "MICH": ["MORELIA", "ZAMORA", "URUAPAN", "LAZARO CARDENAS"],
    "MOR":  ["CUERNAVACA", "CUAUTLA", "JIUTEPEC", "TEMIXCO"],
    "NAY":  ["TEPIC", "BAHIA DE BANDERAS", "COMPOSTELA", "SANTIAGO IXCUINTLA"],
    "NL":   ["MONTERREY", "SAN NICOLAS DE LOS GARZA", "GUADALUPE", "APODACA"],
    "OAX":  ["OAXACA", "SALINA CRUZ", "JUCHITAN", "TUXTEPEC"],
    "PUE":  ["PUEBLA", "TEHUACAN", "SAN MARTIN TEXMELUCAN", "ATLIXCO"],
    "QRO":  ["QUERETARO", "SAN JUAN DEL RIO", "CORREGIDORA", "EL MARQUES"],
    "QROO": ["CANCUN", "PLAYA DEL CARMEN", "COZUMEL", "CHETUMAL"],
    "SLP":  ["SAN LUIS POTOSI", "SOLEDAD DE GRACIANO SANCHEZ", "MATEHUALA", "CIUDAD VALLES"],
    "SIN":  ["CULIACAN", "MAZATLAN", "LOS MOCHIS", "GUASAVE"],
    "SON":  ["HERMOSILLO", "NOGALES", "CIUDAD OBREGON", "NAVOJOA"],
    "TAB":  ["VILLAHERMOSA", "CARDENAS", "MACUSPANA", "COMALCALCO"],
    "TAMPS":["TAMPICO", "REYNOSA", "MATAMOROS", "NUEVO LAREDO"],
    "TLAX": ["TLAXCALA", "APIZACO", "CHIAUTEMPAN", "HUAMANTLA"],
    "VER":  ["VERACRUZ", "XALAPA", "COATZACOALCOS", "ORIZABA"],
    "YUC":  ["MERIDA", "VALLADOLID", "TIZIMIN", "PROGRESO"],
    "ZAC":  ["ZACATECAS", "GUADALUPE", "FRESNILLO", "JEREZ"],
}

# Flat list of all municipalities — used by generar_domicilios()
MUNICIPIOS = [m for muns in OFFICES_BY_STATE.values() for m in muns]

def _hash_office_id(office_id: str) -> str:
    """MD5 hash of the office_id string — matches dbt's dbt_utils.generate_surrogate_key() output."""
    return hashlib.md5(office_id.encode()).hexdigest()[:10]

# Build office lookup:  office_id → {state, municipality, office_name}
OFFICES: dict[str, dict] = {
    f"{state}_{mun.replace(' ', '_')}": {
        "office_id":        _hash_office_id(f"{state}_{mun.replace(' ', '_')}"),
        "office_key":       f"{state}_{mun.replace(' ', '_')}",   # human-readable
        "state":            state,
        "municipality":     mun,
        "office_name":      f"Oficina {state} - {mun.title()}",
    }
    for state, muns in OFFICES_BY_STATE.items()
    for mun in muns
}

# List of all office_ids for random sampling
OFFICE_IDS: list[str] = list(OFFICES.keys())


# ─────────────────────────── HELPERS ──────────────────────────────────────────
def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))

def fmt(d):
    """Format date or return empty string."""
    return d.strftime("%Y-%m-%d") if d else ""

def folio_consulta_otorgante():
    return f"{random.randint(10000000000, 99999999999)}-{random.randint(10000, 99999)}"

def folio_consulta():
    return str(random.randint(1000000000, 9999999999))

def clave_otorgante():
    return str(random.randint(1000000000, 9999999999))

def rfc_persona_fisica(nombre, ap_paterno, ap_materno, fecha_nac: date):
    """RFC simplificado (no válido ante SAT, solo para pruebas)."""
    ini = (ap_paterno[:2] + ap_materno[0] + nombre[0]).upper()
    fecha = fecha_nac.strftime("%y%m%d")
    homo = "".join([random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ1234567890") for _ in range(3)])
    return f"{ini}{fecha}{homo}"

def curp_fake(nombre, ap_paterno, ap_materno, fecha_nac: date, sexo: str):
    ini = (ap_paterno[:2] + ap_materno[0] + nombre[0]).upper()
    fecha = fecha_nac.strftime("%y%m%d")
    sx = sexo
    estado = random.choice(ESTADOS_MX)[:2]
    tail = "".join([random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(2)])
    digit = str(random.randint(0, 9))
    return f"{ini}{fecha}{sx}{estado}{tail}{digit}"

def historico_pagos(n_pagos: int, tiene_atrasos: bool) -> str:
    """Genera cadena de histórico de pagos al estilo Círculo de Crédito.
    V = al corriente, 01-99 = días de atraso, - = sin info."""
    pagos = []
    for _ in range(n_pagos):
        if tiene_atrasos and random.random() < 0.35:
            pagos.append(random.choice(["01", "02", "03", "06"]))
        elif random.random() < 0.1:
            pagos.append("-")
        else:
            pagos.append(" V")
    return "".join(pagos)

# ─────────────────────────── GENERADORES PRINCIPALES ─────────────────────────

def generar_domicilios() -> list[dict]:
    municipio = random.choice(MUNICIPIOS)
    estado = random.choice(ESTADOS_MX)
    colonia = random.choice(COLONIAS)
    cp = str(random.randint(10000, 99999))
    base_date = rand_date(date(2018, 1, 1), date(2023, 12, 31))
    domicilios = []
    n = random.randint(1, 3)
    for i in range(n):
        mz = random.randint(1, 50)
        lt = random.randint(1, 30)
        fecha = base_date - timedelta(days=i * random.randint(30, 400))
        tel = f"9{random.randint(100000000, 999999999)}" if random.random() > 0.4 else ""
        domicilios.append({
            "Direccion": f"LT {lt:02d} MZ {mz:02d} {colonia}",
            "ColoniaPoblacion": colonia,
            "DelegacionMunicipio": municipio,
            "Ciudad": municipio,
            "Estado": estado,
            "CP": cp,
            "FechaResidencia": fmt(fecha),
            "NumeroTelefono": tel,
            "TipoDomicilio": random.choice(TIPO_DOMICILIO),
            "TipoAsentamiento": random.choice(["0", "1", ""]),
            "FechaRegistroDomicilio": fmt(fecha),
        })
    return domicilios

def generar_cuenta(fecha_apertura_max: date) -> dict:
    otorgante = random.choice(OTORGANTES)
    tipo_cred = random.choice(list(TIPO_CREDITO.keys()))
    tipo_cuenta = random.choice(list(TIPO_CUENTA.keys()))
    tipo_resp = random.choice(list(TIPO_RESPONSABILIDAD.keys()))
    freq = random.choice(list(FRECUENCIA_PAGOS.keys()))

    apertura = rand_date(date(2010, 1, 1), fecha_apertura_max)
    ultimo_pago = rand_date(apertura, min(apertura + timedelta(days=900), date(2023, 12, 31)))
    ultima_compra = rand_date(apertura, min(apertura + timedelta(days=180), date(2023, 12, 31)))
    fecha_reporte = ultimo_pago + timedelta(days=random.randint(1, 30))
    cerrada = random.random() < 0.35
    fecha_cierre = fmt(ultimo_pago + timedelta(days=random.randint(1, 60))) if cerrada else ""

    credito_max = random.choice([5000, 8000, 10000, 15000, 20000, 30000, 50000, 100000])
    saldo_actual = random.randint(0, credito_max) if not cerrada else 0
    saldo_vencido_val = 0
    n_pagos_vencidos = 0
    tiene_atrasos = random.random() < 0.3
    peor_atraso = 0
    fecha_peor_atraso = ""
    saldo_vencido_peor = 0

    if tiene_atrasos:
        saldo_vencido_val = random.randint(100, min(saldo_actual + 1000, credito_max))
        n_pagos_vencidos = random.randint(1, 6)
        peor_atraso = random.randint(1, 90)
        fecha_peor_atraso = fmt(rand_date(apertura, fecha_reporte))
        saldo_vencido_peor = saldo_vencido_val + random.randint(0, 500)

    n_pagos = random.randint(1, 24)
    hist = historico_pagos(n_pagos, tiene_atrasos)

    monto_pagar = random.randint(200, 5000) if not cerrada else 0
    clave_prev = random.choice(CLAVE_PREVENCION) if cerrada else ""

    return {
        "FechaActualizacion": fmt(fecha_reporte),
        "RegistroImpugnado": "0",
        "ClaveOtorgante": "",
        "NombreOtorgante": otorgante,
        "CuentaActual": f"{random.randint(10000000000, 99999999999)}-{random.randint(0, 99):02d}" if random.random() > 0.4 else "",
        "TipoResponsabilidad": tipo_resp,
        "TipoCuenta": tipo_cuenta,
        "TipoCredito": tipo_cred,
        "ClaveUnidadMonetaria": "MX",
        "ValorActivoValuacion": str(random.randint(0, credito_max)),
        "NumeroPagos": str(n_pagos),
        "FrecuenciaPagos": freq,
        "MontoPagar": str(monto_pagar),
        "FechaAperturaCuenta": fmt(apertura),
        "FechaUltimoPago": fmt(ultimo_pago),
        "FechaUltimaCompra": fmt(ultima_compra),
        "FechaCierreCuenta": fecha_cierre,
        "FechaReporte": fmt(fecha_reporte),
        "UltimaFechaSaldoCero": "",
        "Garantia": "",
        "CreditoMaximo": str(credito_max),
        "SaldoActual": str(saldo_actual),
        "LimiteCredito": str(credito_max),
        "SaldoVencido": str(saldo_vencido_val),
        "NumeroPagosVencidos": str(n_pagos_vencidos),
        "PagoActual": " V" if not tiene_atrasos else "01",
        "HistoricoPagos": hist,
        "FechaRecienteHistoricoPagos": "",
        "FechaAntiguaHistoricoPagos": "",
        "ClavePrevencion": clave_prev,
        "TotalPagosReportados": "0",
        "PeorAtraso": str(peor_atraso),
        "FechaPeorAtraso": fecha_peor_atraso,
        "SaldoVencidoPeorAtraso": str(saldo_vencido_peor),
    }

def generar_consultas(n: int = 2) -> list[dict]:
    consultas = []
    for _ in range(n):
        fecha = rand_date(date(2020, 1, 1), date(2023, 12, 31))
        monto = random.choice([5000, 10000, 15000, 20000, 50000, 100000])
        consultas.append({
            "FechaConsulta": fmt(fecha),
            "ClaveOtorgante": "",
            "NombreOtorgante": random.choice(OTORGANTES),
            "TelefonoOtorgante": f"9{random.randint(100000000, 999999999)} ",
            "TipoCredito": random.choice(["F", "R", "H"]),
            "ClaveUnidadMonetaria": "MX",
            "ImporteCredito": str(monto),
            "TipoResponsabilidad": "",
        })
    return consultas

def generar_score() -> dict:
    valor = random.randint(400, 850)
    razones = random.sample(RAZONES_FICO, 4)
    return {
        "NombreScore": "FICO",
        "Codigo": str(random.randint(20, 30)),
        "Valor": str(valor),
        "Razon1": razones[0],
        "Razon2": razones[1],
        "Razon3": razones[2],
        "Razon4": razones[3],
        "Error": "0",
    }

def generar_persona() -> dict:
    sexo = random.choice(["F", "M"])
    if sexo == "F":
        nombre = fake.first_name_female()
    else:
        nombre = fake.first_name_male()

    ap_paterno = fake.last_name()
    ap_materno = fake.last_name()
    fecha_nac = rand_date(date(1950, 1, 1), date(2000, 12, 31))

    tiene_rfc = random.random() > 0.3
    tiene_curp = random.random() > 0.2

    rfc = rfc_persona_fisica(nombre, ap_paterno, ap_materno, fecha_nac) if tiene_rfc else ""
    curp = curp_fake(nombre, ap_paterno, ap_materno, fecha_nac, sexo) if tiene_curp else ""

    return {
        "folio_otorgante": folio_consulta_otorgante(),
        "clave_otorgante": clave_otorgante(),
        "folio_consulta": folio_consulta(),
        "ApellidoPaterno": ap_paterno,
        "ApellidoMaterno": ap_materno,
        "ApellidoAdicional": "",
        "Nombres": nombre,
        "FechaNacimiento": fmt(fecha_nac),
        "RFC": rfc,
        "CURP": curp,
        "Nacionalidad": "MX",
        "Residencia": str(random.randint(1, 9)),
        "EstadoCivil": random.choice(ESTADO_CIVIL),
        "Sexo": sexo,
        "ClaveElectorIFE": "",
        "NumeroDependientes": str(random.randint(0, 5)),
        "FechaDefuncion": "",
        "domicilios": generar_domicilios(),
        "cuentas": [generar_cuenta(date(2023, 12, 31)) for _ in range(random.randint(1, 6))],
        "consultas": generar_consultas(random.randint(1, 3)),
        "score": generar_score(),
    }

# ─────────────────────────── OFFICES CSV ─────────────────────────────────────

def generar_offices_csv(path: str) -> None:
    """
    Write the offices reference table to a CSV file.
    Columns: office_id, state, municipality, office_name
    One row per (state, municipality) combination — 32 states × 4 = 128 offices.
    """
    import csv
    rows = list(OFFICES.values())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["office_id", "state", "municipality", "office_name", "office_key"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"🏢  Offices CSV saved : {path}  ({len(rows)} offices)")


# ─────────────────────────── SOLICITUDES / APPLICATION RECORDS ───────────────

def pareto_disbursement(
    x_min: float = 1_000.0,
    alpha: float = 1.16,
    cap: float = 5_000_000.0,
) -> float:
    """
    Sample from a Pareto (power-law) distribution.

    Parameters
    ----------
    x_min  : minimum disbursement amount (scale / location)
    alpha  : shape parameter — lower alpha = heavier tail (more extreme outliers)
             1.16 approximates the classic 80/20 Pareto principle
    cap    : hard ceiling to avoid astronomically large values in test data

    Formula : x = x_min / U^(1/alpha)   where U ~ Uniform(0, 1)
    """
    u = random.random()
    sample = x_min / (u ** (1.0 / alpha))
    return round(min(sample, cap), 2)


def generar_application(
    archivo: str,
    curp: str | None,
    fecha_inicio: date,
    fecha_fin: date,
) -> dict:
    """
    Generate one application record linked to a bureau XML file.

    Fields
    ------
    application_uuid   : UUID v4
    fecha_solicitud    : random date in [fecha_inicio, fecha_fin] — uniform distribution
    monto_dispersion   : disbursement amount sampled from a Pareto distribution
    curp               : CURP of the first persona in the associated XML
    archivo_xml        : filename of the associated XML
    office_id          : randomly assigned office (state_municipality key from OFFICES catalogue)
    """
    raw_office_id = random.choice(OFFICE_IDS)
    
    return {
        "application_uuid": str(uuid.uuid4()),
        "fecha_solicitud":  str(rand_date(fecha_inicio, fecha_fin)),
        "monto_dispersion": pareto_disbursement(),
        "curp":             curp or "",
        "archivo_xml":      archivo,
        "office_id": _hash_office_id(raw_office_id),
    }


# ─────────────────────────── CONSTRUCCIÓN DEL XML ─────────────────────────────


# ── Custom XML renderer (avoids ElementTree tag-abbreviation quirks like Error→e)
def xescape(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def build_xml(personas: list[dict]) -> str:
    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<Respuesta xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="/Respuesta.xsd">')
    out.append("  <Personas>")

    for p in personas:
        out.append("    <Persona>")

        # ── Encabezado
        out.append("      <Encabezado>")
        for k, v in [("FolioConsultaOtorgante", p["folio_otorgante"]),
                     ("ClaveOtorgante",          p["clave_otorgante"]),
                     ("ExpedienteEncontrado",    "1"),
                     ("FolioConsulta",           p["folio_consulta"])]:
            out.append(f"        <{k}>{xescape(v)}</{k}>")
        out.append("      </Encabezado>")

        # ── Nombre
        out.append("      <Nombre>")
        for campo in ["ApellidoPaterno","ApellidoMaterno","ApellidoAdicional","Nombres",
                      "FechaNacimiento","RFC","CURP","Nacionalidad","Residencia",
                      "EstadoCivil","Sexo","ClaveElectorIFE","NumeroDependientes","FechaDefuncion"]:
            v = p.get(campo, "")
            out.append(f"        <{campo}>{xescape(v)}</{campo}>" if v else f"        <{campo}/>")
        out.append("      </Nombre>")

        # ── Domicilios
        out.append("      <Domicilios>")
        for d in p["domicilios"]:
            out.append("        <Domicilio>")
            for k, v in d.items():
                out.append(f"          <{k}>{xescape(v)}</{k}>" if v else f"          <{k}/>")
            out.append("        </Domicilio>")
        out.append("      </Domicilios>")

        out.append("      <Empleos/>")

        # ── Mensajes
        out.append("      <Mensajes>")
        out.append("        <Mensaje>")
        out.append("          <TipoMensaje>2</TipoMensaje>")
        out.append("          <Leyenda>1</Leyenda>")
        out.append("        </Mensaje>")
        out.append("      </Mensajes>")

        # ── Cuentas
        out.append("      <Cuentas>")
        for c in p["cuentas"]:
            out.append("        <Cuenta>")
            for k, v in c.items():
                out.append(f"          <{k}>{xescape(v)}</{k}>" if v != "" else f"          <{k}/>")
            out.append("        </Cuenta>")
        out.append("      </Cuentas>")

        # ── Consultas
        out.append("      <ConsultasEfectuadas>")
        for q in p["consultas"]:
            out.append("        <ConsultaEfectuada>")
            for k, v in q.items():
                out.append(f"          <{k}>{xescape(v)}</{k}>" if v else f"          <{k}/>")
            out.append("        </ConsultaEfectuada>")
        out.append("      </ConsultasEfectuadas>")

        out.append("      <BlackList/>")
        out.append("      <DeclaracionesConsumidor/>")

        # ── Scores
        s = p["score"]
        out.append("      <Scores>")
        out.append("        <Score>")
        for k in ["NombreScore","Codigo","Valor","Razon1","Razon2","Razon3","Razon4","Error"]:
            out.append(f"          <{k}>{xescape(s[k])}</{k}>")
        out.append("        </Score>")
        out.append("      </Scores>")

        out.append("    </Persona>")

    out.append("  </Personas>")
    out.append("</Respuesta>")
    return "\n".join(out)


# ─────────────────────────── GCS UPLOAD ──────────────────────────────────────

def _gcs_client(credentials_path=None, project=None):
    """
    Build an authenticated GCS client.

    Resolution order for credentials:
      1. --gcs-credentials CLI argument (explicit service account JSON)
      2. GOOGLE_APPLICATION_CREDENTIALS environment variable
      3. gcloud application-default credentials

    Resolution order for project:
      1. --gcs-project CLI argument
      2. Inferred from the service account JSON (if provided)
      3. gcloud default project
    """
    import os, json
    from google.oauth2 import service_account as sa

    if credentials_path:
        key_path = os.path.expanduser(credentials_path)
        creds = sa.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        if project is None:
            with open(key_path) as f:
                project = json.load(f).get("project_id")
        return gcs.Client(project=project, credentials=creds)

    return gcs.Client(project=project)


def upload_to_gcs(bucket_name, local_path, gcs_prefix="", credentials_path=None, project=None):
    """
    Upload a single local file to a GCS bucket.
    Returns the gs:// URI of the uploaded object.

    Parameters
    ----------
    bucket_name      : GCS bucket name (without gs:// prefix)
    local_path       : path to the local file
    gcs_prefix       : optional folder prefix inside the bucket (e.g. "xmls/")
    credentials_path : path to service account JSON — overrides ADC if provided
    project          : GCP project ID — inferred from JSON if not set
    """
    import os
    if not GCS_AVAILABLE:
        raise ImportError(
            "google-cloud-storage is not installed. "
            "Run: pip install google-cloud-storage"
        )
    client      = _gcs_client(credentials_path, project)
    bucket      = client.bucket(bucket_name)
    object_name = gcs_prefix.rstrip("/") + "/" + os.path.basename(local_path) \
                  if gcs_prefix else os.path.basename(local_path)
    blob        = bucket.blob(object_name)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{object_name}"


def upload_folder_to_gcs(bucket_name, local_paths, gcs_prefix="xmls", credentials_path=None, project=None):
    """Upload a list of local file paths to GCS in sequence, printing progress."""
    total = len(local_paths)
    for i, path in enumerate(local_paths, 1):
        uri = upload_to_gcs(bucket_name, path, gcs_prefix, credentials_path, project)
        print(f"  ☁  [{i:>4}/{total}] {uri}")


# ─────────────────────────── CLI ──────────────────────────────────────────────


def main():
    import os

    parser = argparse.ArgumentParser(
        description="Genera XMLs de prueba con formato Círculo de Crédito (México)"
    )
    parser.add_argument("--cantidad",     "-n", type=int,  default=1,    help="Personas por XML (default: 1)")
    parser.add_argument("--archivos",     "-a", type=int,  default=1,    help="Número de XMLs a generar (default: 1)")
    parser.add_argument("--salida",       "-o", type=str,  default=None, help="Nombre base del archivo de salida")
    parser.add_argument("--destino",      "-d", type=str,  default=None, help="Carpeta de destino (se crea si no existe)")
    parser.add_argument("--seed",               type=int,  default=None, help="Semilla para reproducibilidad")
    # ── application records
    parser.add_argument("--applications",       type=str,  default=None,
                        help="Path to the JSON file for application records (e.g. applications.json)")
    parser.add_argument("--fecha-inicio",       type=str,  default="2023-01-01",
                        help="Fecha de inicio para solicitudes (YYYY-MM-DD, default: 2023-01-01)")
    parser.add_argument("--fecha-fin",          type=str,  default="2023-12-31",
                        help="Fecha de fin para solicitudes (YYYY-MM-DD, default: 2023-12-31)")
    parser.add_argument("--offices",            type=str,  default=None,
                        help="Path to write the offices reference CSV (e.g. offices.csv)")
    # ── zip
    parser.add_argument("--zip",                type=str,  default=None,
                        help="Empaqueta todos los XMLs generados en este archivo ZIP (ej: xmls.zip)")
    # ── gcs
    parser.add_argument("--gcs-bucket",         type=str,  default=None,
                        help="Nombre del bucket GCS donde subir los XMLs y el CSV (eg: de-zoomcamp-project-xmls-bucket)")
    parser.add_argument("--gcs-xml-prefix",     type=str,  default="xmls",
                        help="Prefijo/carpeta dentro del bucket para los XMLs (default: xmls)")
    parser.add_argument("--gcs-csv-prefix",     type=str,  default="",
                        help="Prefijo/carpeta dentro del bucket para el CSV (default: raíz del bucket)")
    parser.add_argument("--gcs-credentials",    type=str,  default=None,
                        help="Ruta al JSON de cuenta de servicio GCP (ej: ~/.gcp/key.json). "
                             "Si no se indica, se usa GOOGLE_APPLICATION_CREDENTIALS o gcloud ADC.")
    parser.add_argument("--gcs-project",        type=str,  default=None,
                        help="GCP project ID. Se infiere del JSON si no se indica.")
    args = parser.parse_args()

    # ── resolve destination folder
    if args.destino:
        destino = os.path.abspath(args.destino)
        os.makedirs(destino, exist_ok=True)
        print(f"📁  Carpeta de destino : {destino}")
    else:
        destino = os.getcwd()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    # ── parse date window
    fecha_inicio = date.fromisoformat(args.fecha_inicio)
    fecha_fin    = date.fromisoformat(args.fecha_fin)

    # ── generate offices CSV if requested
    if args.offices:
        generar_offices_csv(os.path.abspath(args.offices))

    applications = []  # accumulates application record dicts

    # ── open ZIP upfront if requested — XMLs are written directly into it,
    #    no individual files ever land on disk
    zip_path  = os.path.abspath(args.zip) if args.zip else None
    zf_handle = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED)                 if zip_path else None

    if zip_path:
        print(f"🗜   ZIP en memoria       : {zip_path}")

    try:
        for i in range(1, args.archivos + 1):
            personas    = [generar_persona() for _ in range(args.cantidad)]
            xml_content = build_xml(personas)

            # ── build filename
            if args.salida and args.archivos == 1:
                nombre_base = args.salida if args.salida.endswith(".xml") else args.salida + ".xml"
            elif args.salida:
                nombre_base = f"{args.salida}_{i:04d}.xml"
            else:
                nombre_base = f"reporte_{i:04d}.xml"

            if zf_handle:
                # write directly into the ZIP — no file ever hits the filesystem
                zf_handle.writestr(nombre_base, xml_content.encode("utf-8"))
                print(f"  ✅  [{i:>5}/{args.archivos}] {nombre_base} → ZIP")
            else:
                # normal file write
                ruta_completa = os.path.join(destino, nombre_base)
                with open(ruta_completa, "w", encoding="utf-8") as f:
                    f.write(xml_content)
                print(f"  ✅  [{i:>5}/{args.archivos}] {ruta_completa}")

            # ── application record — one per XML, linked to first persona's CURP
            if args.applications:
                first_curp = personas[0].get("CURP") or None
                app = generar_application(nombre_base, first_curp, fecha_inicio, fecha_fin)
                applications.append(app)
                print(f"         ↳ solicitud {app['application_uuid'][:8]}...  "
                      f"monto={app['monto_dispersion']:>12,.2f}  "
                      f"fecha={app['fecha_solicitud']}  "
                      f"curp={app['curp'] or '(no curp)'}")

    finally:
        # always close the ZIP cleanly, even if interrupted
        if zf_handle:
            zf_handle.close()
            total_kb = os.path.getsize(zip_path) // 1024
            print(f"\n🗜   ZIP cerrado           : {zip_path}  ({args.archivos} archivos, {total_kb} KB)")

    # ── save applications JSON
    if args.applications and applications:
        import json
        json_path = os.path.abspath(args.applications)

        # append to existing file if it already exists, otherwise start fresh
        existing = []
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)

        existing.extend(applications)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)

        print(f"\n📊  Applications saved   : {json_path}  ({len(existing)} total records)")
        for app in applications:
            print(f"     {app['application_uuid'][:8]}...  "
                  f"amount={app['monto_dispersion']:>12,.2f}  "
                  f"date={app['fecha_solicitud']}  "
                  f"curp={app['curp'] or '(no curp)'}")

    # ── upload to GCS
    if args.gcs_bucket:
        if not GCS_AVAILABLE:
            print("\n⚠️   google-cloud-storage not installed — skipping GCS upload.")
            print("    Run: pip install google-cloud-storage")
        else:
            print(f"\n☁   Subiendo a gs://{args.gcs_bucket}/...")
            # ZIP (contains all XMLs — upload the single file instead of N individual files)
            if zip_path:
                uri = upload_to_gcs(args.gcs_bucket, zip_path, args.gcs_xml_prefix, args.gcs_credentials, args.gcs_project)
                print(f"  ☁   ZIP → {uri}")
            # JSON applications file
            if args.applications and applications:
                json_path_upload = os.path.abspath(args.applications)
                uri = upload_to_gcs(args.gcs_bucket, json_path_upload, args.gcs_csv_prefix, args.gcs_credentials, args.gcs_project)
                print(f"  ☁   JSON → {uri}")
            # offices CSV
            if args.offices:
                offices_path_upload = os.path.abspath(args.offices)
                uri = upload_to_gcs(args.gcs_bucket, offices_path_upload, args.gcs_csv_prefix, args.gcs_credentials, args.gcs_project)
                print(f"  ☁   offices CSV → {uri}")
            print("  ✅  Subida completada")

    print("\nCatálogos utilizados:")
    print(f"  TipoCredito     : {', '.join(TIPO_CREDITO.keys())}  ({', '.join(TIPO_CREDITO.values())})")
    print(f"  TipoCuenta      : {', '.join(TIPO_CUENTA.keys())}  ({', '.join(TIPO_CUENTA.values())})")
    print(f"  TipoResp.       : {', '.join(TIPO_RESPONSABILIDAD.keys())}  ({', '.join(TIPO_RESPONSABILIDAD.values())})")
    print(f"  FrecuenciaPagos : {', '.join(FRECUENCIA_PAGOS.keys())}  ({', '.join(FRECUENCIA_PAGOS.values())})")

if __name__ == "__main__":
    main()