"""
dlt Pipeline — Círculo de Crédito (GCS) → BigQuery
===================================================
Reads three sources from Google Cloud Storage and loads them into
the BigQuery dataset `raw`:

    raw.personas          — demographic data + FICO score          (from ZIP of XMLs)
    raw.domicilios        — address history      (1-N per person)  (from ZIP of XMLs)
    raw.cuentas           — reported credit accounts (1-N per person) (from ZIP of XMLs)
    raw.consultas         — credit inquiries made (1-N per person)  (from ZIP of XMLs)
    raw.applications      — loan application records + office_id   (from applications.json)

Every XML-derived table carries folio_consulta, curp, and rfc as join keys.

How it works
------------
- XMLs: the ZIP is downloaded into memory, never written to disk.
- applications.json: downloaded from GCS and parsed as a JSON array.
All three share the same GCS credentials.

Usage
-----
    python ingestar_circulo_credito.py
    python ingestar_circulo_credito.py --zip gs://bucket/xmls/bureau.zip
    python ingestar_circulo_credito.py --applications gs://bucket/applications.json
    python ingestar_circulo_credito.py --credentials ~/.gcp/key.json --disposition replace

Credentials resolution order
-----------------------------
1. --credentials CLI argument  (service account JSON path)
2. GOOGLE_APPLICATION_CREDENTIALS environment variable
3. gcloud application-default login  (~/.config/gcloud/...)

Dependencies
------------
    uv add "dlt[bigquery]" xmltodict
"""

import argparse
import io
import json
import os
import zipfile
from typing import Iterator

import dlt
import xmltodict
from dlt.common.typing import TDataItems


# ─────────────────────────── helpers ──────────────────────────────────────────

def _val(v) -> str | None:
    """xmltodict returns None for empty tags and dicts for nested elements.
    We only want scalar strings."""
    if v is None or isinstance(v, dict):
        return None
    return str(v).strip() or None


def _ensure_list(obj) -> list:
    """xmltodict returns a plain dict (not a list) when there is only one child."""
    if obj is None:
        return []
    return obj if isinstance(obj, list) else [obj]


# ─────────────────────────── credentials ──────────────────────────────────────

def _load_gcp_credentials(credentials_path: str | None):
    """
    Return (google.oauth2.Credentials, project_id).
    Falls back to Application Default Credentials if no path is given.
    """
    import google.auth
    from google.oauth2 import service_account

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    if credentials_path:
        path = os.path.expanduser(credentials_path)
        creds = service_account.Credentials.from_service_account_file(path, scopes=scopes)
        with open(path) as f:
            project = json.load(f).get("project_id")
        return creds, project

    creds, project = google.auth.default(scopes=scopes)
    return creds, project


# ─────────────────────────── GCS download helper ──────────────────────────────

def _download_gcs_bytes(gcs_url: str, credentials_path: str | None) -> bytes:
    """
    Download any GCS object into memory and return raw bytes.
    Parses gs://bucket/path/to/object correctly without URL-encoding issues.
    """
    from google.cloud import storage as gcs_lib

    creds, project = _load_gcp_credentials(credentials_path)
    client = gcs_lib.Client(project=project, credentials=creds)

    without_scheme = gcs_url.removeprefix("gs://")
    bucket_name, _, object_name = without_scheme.partition("/")

    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(object_name)

    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────── ZIP → XML reader ─────────────────────────────────

def _iter_xmls_from_gcs_zip(
    gcs_zip_url: str,
    credentials_path: str | None,
) -> Iterator[tuple[str, bytes]]:
    """
    Stream every *.xml file from a ZIP stored in GCS.
    Yields (filename, xml_bytes) — the ZIP is never written to disk.
    """
    print(f"  📦 Downloading ZIP from GCS: {gcs_zip_url}")
    raw_bytes = _download_gcs_bytes(gcs_zip_url, credentials_path)
    print(f"  ✔  Downloaded {len(raw_bytes):,} bytes")

    with zipfile.ZipFile(io.BytesIO(raw_bytes), "r") as zf:
        xml_members = [m for m in zf.namelist() if m.lower().endswith(".xml")]
        print(f"  📄 Found {len(xml_members)} XML files inside the ZIP")
        for name in xml_members:
            yield name, zf.read(name)


# ─────────────────────────── flatteners ───────────────────────────────────────

def _flatten_persona(p: dict, folio: str, archivo: str) -> dict:
    n         = p.get("Nombre") or {}
    enc       = p.get("Encabezado") or {}
    score_raw = (p.get("Scores") or {}).get("Score") or {}
    return {
        "folio_consulta":           folio,
        "archivo_origen":           archivo,
        "folio_consulta_otorgante": _val(enc.get("FolioConsultaOtorgante")),
        "clave_otorgante":          _val(enc.get("ClaveOtorgante")),
        "expediente_encontrado":    _val(enc.get("ExpedienteEncontrado")),
        "apellido_paterno":         _val(n.get("ApellidoPaterno")),
        "apellido_materno":         _val(n.get("ApellidoMaterno")),
        "apellido_adicional":       _val(n.get("ApellidoAdicional")),
        "nombres":                  _val(n.get("Nombres")),
        "fecha_nacimiento":         _val(n.get("FechaNacimiento")),
        "rfc":                      _val(n.get("RFC")),
        "curp":                     _val(n.get("CURP")),
        "nacionalidad":             _val(n.get("Nacionalidad")),
        "residencia":               _val(n.get("Residencia")),
        "estado_civil":             _val(n.get("EstadoCivil")),
        "sexo":                     _val(n.get("Sexo")),
        "clave_elector_ife":        _val(n.get("ClaveElectorIFE")),
        "numero_dependientes":      _val(n.get("NumeroDependientes")),
        "fecha_defuncion":          _val(n.get("FechaDefuncion")),
        "score_nombre":             _val(score_raw.get("NombreScore")),
        "score_codigo":             _val(score_raw.get("Codigo")),
        "score_valor":              _val(score_raw.get("Valor")),
        "score_razon_1":            _val(score_raw.get("Razon1")),
        "score_razon_2":            _val(score_raw.get("Razon2")),
        "score_razon_3":            _val(score_raw.get("Razon3")),
        "score_razon_4":            _val(score_raw.get("Razon4")),
        "score_error":              _val(score_raw.get("Error")),
    }


def _flatten_domicilio(d: dict, folio: str, curp: str | None, rfc: str | None, idx: int) -> dict:
    return {
        "folio_consulta":       folio,
        "curp":                 curp,
        "rfc":                  rfc,
        "domicilio_idx":        idx,
        "direccion":            _val(d.get("Direccion")),
        "colonia_poblacion":    _val(d.get("ColoniaPoblacion")),
        "delegacion_municipio": _val(d.get("DelegacionMunicipio")),
        "ciudad":               _val(d.get("Ciudad")),
        "estado":               _val(d.get("Estado")),
        "cp":                   _val(d.get("CP")),
        "fecha_residencia":     _val(d.get("FechaResidencia")),
        "numero_telefono":      _val(d.get("NumeroTelefono")),
        "tipo_domicilio":       _val(d.get("TipoDomicilio")),
        "tipo_asentamiento":    _val(d.get("TipoAsentamiento")),
        "fecha_registro":       _val(d.get("FechaRegistroDomicilio")),
    }


def _flatten_cuenta(c: dict, folio: str, curp: str | None, rfc: str | None, idx: int) -> dict:
    return {
        "folio_consulta":            folio,
        "curp":                      curp,
        "rfc":                       rfc,
        "cuenta_idx":                idx,
        "fecha_actualizacion":       _val(c.get("FechaActualizacion")),
        "registro_impugnado":        _val(c.get("RegistroImpugnado")),
        "nombre_otorgante":          _val(c.get("NombreOtorgante")),
        "cuenta_actual":             _val(c.get("CuentaActual")),
        "tipo_responsabilidad":      _val(c.get("TipoResponsabilidad")),
        "tipo_cuenta":               _val(c.get("TipoCuenta")),
        "tipo_credito":              _val(c.get("TipoCredito")),
        "clave_unidad_monetaria":    _val(c.get("ClaveUnidadMonetaria")),
        "valor_activo_valuacion":    _val(c.get("ValorActivoValuacion")),
        "numero_pagos":              _val(c.get("NumeroPagos")),
        "frecuencia_pagos":          _val(c.get("FrecuenciaPagos")),
        "monto_pagar":               _val(c.get("MontoPagar")),
        "fecha_apertura_cuenta":     _val(c.get("FechaAperturaCuenta")),
        "fecha_ultimo_pago":         _val(c.get("FechaUltimoPago")),
        "fecha_ultima_compra":       _val(c.get("FechaUltimaCompra")),
        "fecha_cierre_cuenta":       _val(c.get("FechaCierreCuenta")),
        "fecha_reporte":             _val(c.get("FechaReporte")),
        "credito_maximo":            _val(c.get("CreditoMaximo")),
        "saldo_actual":              _val(c.get("SaldoActual")),
        "limite_credito":            _val(c.get("LimiteCredito")),
        "saldo_vencido":             _val(c.get("SaldoVencido")),
        "numero_pagos_vencidos":     _val(c.get("NumeroPagosVencidos")),
        "pago_actual":               _val(c.get("PagoActual")),
        "historico_pagos":           _val(c.get("HistoricoPagos")),
        "clave_prevencion":          _val(c.get("ClavePrevencion")),
        "peor_atraso":               _val(c.get("PeorAtraso")),
        "fecha_peor_atraso":         _val(c.get("FechaPeorAtraso")),
        "saldo_vencido_peor_atraso": _val(c.get("SaldoVencidoPeorAtraso")),
    }


def _flatten_consulta(q: dict, folio: str, curp: str | None, rfc: str | None, idx: int) -> dict:
    return {
        "folio_consulta":       folio,
        "curp":                 curp,
        "rfc":                  rfc,
        "consulta_idx":         idx,
        "fecha_consulta":       _val(q.get("FechaConsulta")),
        "nombre_otorgante":     _val(q.get("NombreOtorgante")),
        "telefono_otorgante":   _val(q.get("TelefonoOtorgante")),
        "tipo_credito":         _val(q.get("TipoCredito")),
        "importe_credito":      _val(q.get("ImporteCredito")),
        "tipo_responsabilidad": _val(q.get("TipoResponsabilidad")),
    }


# ─────────────────────────── dlt resources ────────────────────────────────────

PERSONAS_COLUMNS = {
    "apellido_adicional": {"data_type": "text", "nullable": True},
    "clave_elector_ife":  {"data_type": "text", "nullable": True},
    "fecha_defuncion":    {"data_type": "text", "nullable": True},
}
CONSULTAS_COLUMNS = {
    "tipo_responsabilidad": {"data_type": "text", "nullable": True},
}


@dlt.resource(name="circulo_xml_zip", standalone=True)
def parse_circulo_zip(
    gcs_zip_url: str,
    credentials_path: str | None = None,
) -> Iterator[TDataItems]:
    """Streams XMLs from a GCS ZIP → personas, domicilios, cuentas, consultas."""
    for archivo, xml_bytes in _iter_xmls_from_gcs_zip(gcs_zip_url, credentials_path):
        print(f"    ↳ parsing {archivo}")
        raw = xmltodict.parse(xml_bytes)

        personas_list = _ensure_list(
            (raw.get("Respuesta", {}).get("Personas") or {}).get("Persona")
        )

        for p in personas_list:
            enc   = p.get("Encabezado") or {}
            nom   = p.get("Nombre") or {}
            folio = _val(enc.get("FolioConsulta")) or f"{archivo}_{id(p)}"
            curp  = _val(nom.get("CURP"))
            rfc   = _val(nom.get("RFC"))

            yield dlt.mark.with_hints(
                _flatten_persona(p, folio, archivo),
                dlt.mark.make_hints(table_name="personas", columns=PERSONAS_COLUMNS),
            )
            for idx, d in enumerate(_ensure_list((p.get("Domicilios") or {}).get("Domicilio"))):
                yield dlt.mark.with_hints(
                    _flatten_domicilio(d, folio, curp, rfc, idx),
                    dlt.mark.make_hints(table_name="domicilios"),
                )
            for idx, c in enumerate(_ensure_list((p.get("Cuentas") or {}).get("Cuenta"))):
                yield dlt.mark.with_hints(
                    _flatten_cuenta(c, folio, curp, rfc, idx),
                    dlt.mark.make_hints(table_name="cuentas"),
                )
            for idx, q in enumerate(_ensure_list((p.get("ConsultasEfectuadas") or {}).get("ConsultaEfectuada"))):
                yield dlt.mark.with_hints(
                    _flatten_consulta(q, folio, curp, rfc, idx),
                    dlt.mark.make_hints(table_name="consultas", columns=CONSULTAS_COLUMNS),
                )


@dlt.resource(name="applications", standalone=True)
def load_applications_json(
    gcs_url: str,
    credentials_path: str | None = None,
) -> Iterator[TDataItems]:
    """
    Downloads applications.json from GCS and yields each record as a row
    into raw.applications.

    Expected JSON structure: a top-level array of objects, e.g.:
    [
      {"application_uuid": "...", "fecha_solicitud": "...",
       "monto_dispersion": 1234.56, "curp": "...",
       "archivo_xml": "...", "office_id": "..."},
      ...
    ]
    """
    print(f"  📋 Downloading applications JSON from GCS: {gcs_url}")
    raw_bytes = _download_gcs_bytes(gcs_url, credentials_path)
    records   = json.loads(raw_bytes.decode("utf-8"))
    print(f"  ✔  Loaded {len(records)} application records")
    yield from records



# ─────────────────────────── pipeline builder ─────────────────────────────────

def build_pipeline(
    project: str,
    credentials_path: str | None,
    location: str,
) -> dlt.Pipeline:
    """Build a dlt pipeline targeting BigQuery dataset `raw`."""
    from dlt.destinations import bigquery
    from dlt.sources.credentials import GcpServiceAccountCredentials

    if credentials_path:
        path = os.path.expanduser(credentials_path)
        with open(path) as f:
            sa_info = json.load(f)
        bq_creds = GcpServiceAccountCredentials()
        bq_creds.project_id   = sa_info["project_id"]
        bq_creds.private_key  = sa_info["private_key"]
        bq_creds.client_email = sa_info["client_email"]
    else:
        bq_creds = None

    destination = bigquery(
        project_id=project,
        credentials=bq_creds,
        location=location,
    )

    return dlt.pipeline(
        pipeline_name="circulo_credito_bq",
        destination=destination,
        dataset_name="raw",          # renamed from buro → raw
        dev_mode=False,
    )


def run(
    gcs_zip_url: str,
    gcs_applications_url: str | None,
    project: str,
    credentials_path: str | None,
    disposition: str,
    location: str,
) -> None:
    print(f"\n🚀 Pipeline: GCS → BigQuery (dataset: raw)")
    print(f"   ZIP          : {gcs_zip_url}")
    print(f"   Applications : {gcs_applications_url or '(skipped)'}")
    print(f"   Project      : {project}")
    print(f"   Mode         : {disposition}\n")

    resources = [
        parse_circulo_zip(gcs_zip_url=gcs_zip_url, credentials_path=credentials_path),
    ]

    if gcs_applications_url:
        resources.append(
            load_applications_json(gcs_url=gcs_applications_url, credentials_path=credentials_path)
        )


    pipeline = build_pipeline(project, credentials_path, location)
    info     = pipeline.run(resources, write_disposition=disposition)

    print(f"\n✅ Pipeline complete")
    print(info)
    print(f"\n📊 Tables loaded: {pipeline.last_trace.last_normalize_info}")


# ─────────────────────────── CLI ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Ingest GCS sources → BigQuery dataset `raw`"
    )
    parser.add_argument(
        "--zip", type=str,
        default="gs://de-zoomcamp-project-xmls-bucket/xmls/bureau.zip",
        help="gs:// URL of the XML ZIP in GCS",
    )
    parser.add_argument(
        "--applications", type=str,
        default="gs://de-zoomcamp-project-xmls-bucket/applications.json",
        help="gs:// URL of applications.json in GCS (omit to skip)",
    )
    parser.add_argument(
        "--project", type=str,
        default="de-zoomcamp-project",
        help="GCP project ID",
    )
    parser.add_argument(
        "--credentials", type=str, default=None,
        help="Path to service account JSON. Omit to use ADC.",
    )
    parser.add_argument(
        "--disposition", "-m", type=str, default="replace",
        choices=["append", "replace", "merge"],
        help="dlt write disposition (default: replace)",
    )
    parser.add_argument(
        "--location", type=str, default="US",
        help="BigQuery dataset location (default: US)",
    )
    args = parser.parse_args()

    run(
        gcs_zip_url=args.zip,
        gcs_applications_url=args.applications,
        project=args.project,
        credentials_path=args.credentials,
        disposition=args.disposition,
        location=args.location,
    )


if __name__ == "__main__":
    main()