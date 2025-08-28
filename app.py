from __future__ import annotations
import os
import io
import json
import uuid
import pytz
import queue
import boto3
import threading
import pandas as pd
from io import BytesIO
from functools import wraps
from datetime import datetime
from dotenv import load_dotenv
from segmentador import segmentar
from jinja2 import TemplateNotFound
from typing import Dict, List, Optional, Tuple
from werkzeug.security import check_password_hash
from flask import (Flask, Response, abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for)

###################################### VARIABLES DINÁMICAS ######################################

load_dotenv()

def _parse_clients() -> dict[str, dict]:
    cfg: dict[str, dict] = {}
    for slug in (os.getenv("CLIENTS") or "").split(","):
        slug = slug.strip().upper()
        if not slug:
            continue
        config_slugs = [
            v
            for k, v in os.environ.items()
            if k.startswith(f"CONFIG_SLUG_{slug}_") and v
        ]
        cfg[slug] = {
            "username":      os.getenv(f"ADMIN_USER_{slug}"),
            "password_hash": os.getenv(f"ADMIN_PASSWORD_HASH_{slug}"),
            "display_name":  os.getenv(f"DISPLAY_NAME_{slug}", slug.title()),
            "campana_slug":  os.getenv(f"CAMPANA_SLUG_{slug}", slug.lower()),
            "config_slugs":  config_slugs, 
            "dashboard_config": os.getenv(f"CONFIG_DASHBOARD_{slug}"),            
        }
    return cfg

CLIENTS = _parse_clients()

JOB_RESULTS: Dict[str, dict] = {}

ADMIN_USER          = os.getenv("ADMIN_USER")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH")
SECRET_KEY = os.getenv("SECRET_KEY")
S3_BUCKET  = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
LAMBDA_DASHBOARD  = os.getenv("LAMBDA_DASHBOARD")
S3_BUCKET_CONFIG = os.getenv("S3_BUCKET_CONFIG")

###################################### CONFIGURACIÓN ######################################

app = Flask(__name__,template_folder="src/app/templates",static_folder="src/app/static",)
app.secret_key = SECRET_KEY

s3 = boto3.client("s3", region_name=AWS_REGION)
tz_cdmx = pytz.timezone("America/Mexico_City")
LOG_QUEUES: Dict[str, queue.Queue[Optional[str]]] = {}

###################################### CAMPANAS ######################################

@app.context_processor
def inject_display_name():
    return dict(display_name=session.get("display_name", ""))

###################################### LOGIN ######################################

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

@app.route("/", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("inicio"))
    if request.method == "POST":
        user_in = request.form["username"]
        pwd_in = request.form["password"]
        for slug, data in CLIENTS.items():
            if (
                user_in == data["username"]
                and data["password_hash"]
                and check_password_hash(data["password_hash"], pwd_in)
            ):
                session.update(
                    {
                        "logged_in": True,
                        "client_slug": slug,
                        "display_name": data["display_name"],
                        "campana": data["campana_slug"],
                    }
                )
                flash("¡Bienvenid@!", "success")
                return redirect(url_for("inicio"))
        flash("Credenciales inválidas", "error")
    return render_template("login.html", page="login")

###################################### LOG OUT ######################################

@app.route("/logout")
@login_required
def logout():
    session.clear()
    flash("Has cerrado sesión", "warning")
    return redirect(url_for("login"))

###################################### INICIO ######################################

@app.route("/inicio")
@login_required
def inicio():
    return render_template("inicio.html", page="inicio")

###################################### 1. DASHBOARD ######################################

def _dashboard_config_file_name(client_slug: Optional[str]) -> Optional[str]:
    if not client_slug:
        return None
    cli_cfg = CLIENTS.get((client_slug or "").upper(), {})
    return cli_cfg.get("dashboard_config")

############################################################################

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", page="dashboard")

############################################################################

def _compute_kpis(data_gestiones: Optional[pd.DataFrame]) -> Optional[dict]:
    if not isinstance(data_gestiones, pd.DataFrame) or data_gestiones.empty:
        return None
    df = data_gestiones.copy()
    contactable = df.get("contactable")
    if contactable is not None:
        try:
            if contactable.dtype == bool:
                mask_contactable = contactable.fillna(False)
            elif pd.api.types.is_numeric_dtype(contactable):
                mask_contactable = contactable.fillna(0).astype(int).astype(bool)
            else:
                s = contactable.astype(str).str.strip().str.lower()
                mask_contactable = s.isin(
                    {"1","si","sí","true","t","contactable","contactado"}
                ).fillna(False)
        except Exception:
            mask_contactable = pd.Series(False, index=df.index)
    else:
        mask_contactable = pd.Series(False, index=df.index)
    subdictamen = df.get("subdictamen")
    if subdictamen is not None:
        sdi = subdictamen.astype(str).str.strip().str.casefold()
        mask_ilocalizable = sdi.eq("cuenta ilocalizable")
    else:
        mask_ilocalizable = pd.Series(False, index=df.index)
    def nunique_safe(series_name: str, msk=None) -> Optional[int]:
        if series_name not in df.columns:
            return None
        s = df[series_name]
        if msk is not None:
            s = s[msk]
        try:
            return int(s.nunique(dropna=True))
        except Exception:
            return None
    kpis = {
        "envios_totales": int(len(df)),
        "eficiencia_global": float(mask_contactable.mean()) if len(df) else None,  # 0..1
        "creditos_contactados": nunique_safe("credit_id", mask_contactable),
        "telefonos_contactados": nunique_safe("phone_number", mask_contactable),
        "cuentas_ilocalizables": nunique_safe("credit_id", mask_ilocalizable),
    }
    return kpis

def _run_dashboard_job(job_id: str, client_slug: Optional[str]):
    q = LOG_QUEUES.setdefault(job_id, queue.Queue())
    def log(line: str):
        try:
            q.put(line)
        except Exception:
            pass
    try:
        log(">>> 1/6. Resolviendo configuración...")
        config_file_name = _dashboard_config_file_name(client_slug)
        if not config_file_name:
            JOB_RESULTS[job_id] = {"error": "No se encontró configuración de Dashboard para este cliente."}
            log("Sin configuración. Abortando.")
            return

        log(f">>> 2/6. Invocando Lambda...")
        lambda_client = boto3.client("lambda", region_name=AWS_REGION)
        event = {"CONFIG_FILE_NAME": config_file_name}
        resp = lambda_client.invoke(
            FunctionName=LAMBDA_DASHBOARD,
            InvocationType="RequestResponse",
            Payload=json.dumps(event),
        )
        payload = json.loads(resp["Payload"].read())

        status   = payload.get("status")
        message  = payload.get("message")
        campaign = payload.get("campaign")
        s3_uri   = payload.get("s3_uri")

        log(f">>> 3/6. Respuesta: {status} — {message}")
        if status != "Exitoso" or not s3_uri:
            JOB_RESULTS[job_id] = {
                "fecha_actual": datetime.now(tz_cdmx).strftime("Reporte actualizado al %d/%m/%Y a las %H:%M hrs."),
                "report": {"status": status, "message": message, "campaign": campaign, "s3_uri": s3_uri, "config_file_name": config_file_name},
                "data_gestiones_shape": None,
                "data_reporte_operativo_shape": None,
            }
            log("Lambda no regresó OK. Abortando.")
            return

        log(">>> 4/6. Descargando dataset...")
        _, _, rest = s3_uri.partition("s3://")
        bucket, _, key = rest.partition("/")
        s3_local = boto3.client("s3", region_name=AWS_REGION)
        obj = s3_local.get_object(Bucket=bucket, Key=key)

        log(">>> 5/6. Procesando hojas...")
        dfs = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name=None)
        data_gestiones = dfs.get("data_gestiones")
        data_reporte_operativo = dfs.get("data_reporte_operativo")
        kpis = _compute_kpis(data_gestiones)

        JOB_RESULTS[job_id] = {
            "fecha_actual": datetime.now(tz_cdmx).strftime("Reporte actualizado al %d/%m/%Y a las %H:%M hrs."),
            "report": {"status": status, "message": message, "campaign": campaign, "s3_uri": s3_uri, "config_file_name": config_file_name},
            "data_gestiones_shape": tuple(data_gestiones.shape) if isinstance(data_gestiones, pd.DataFrame) else None,
            "data_reporte_operativo_shape": tuple(data_reporte_operativo.shape) if isinstance(data_reporte_operativo, pd.DataFrame) else None,
            "kpis": kpis,
        }
        log(">>> 6/6. Proceso completado.")
    except Exception as err:
        JOB_RESULTS[job_id] = {"error": f"Ocurrió un error: {err}"}
        log(f"Error: {err}")
    finally:
        try:
            q.put(None)
        except Exception:
            pass
    
############################################################################

@app.route("/dashboard/run", methods=["POST"])
@login_required
def dashboard_run():
    job_id = uuid.uuid4().hex
    client_slug = session.get("client_slug") 
    LOG_QUEUES[job_id] = queue.Queue()
    t = threading.Thread(target=_run_dashboard_job, args=(job_id, client_slug), daemon=True)
    t.start()
    return jsonify(job_id=job_id)

############################################################################

@app.route("/dashboard/result/<job_id>")
@login_required
def dashboard_result(job_id: str):
    res = JOB_RESULTS.get(job_id)
    if not res:
        return jsonify(status="pending"), 202
    try:
        JOB_RESULTS.pop(job_id, None)
        LOG_QUEUES.pop(job_id, None)
    except Exception:
        pass
    return jsonify(status="Exitoso", **res)

###################################### 2. ORQUESTACIÓN MANUAL ######################################

###################################### 2.1. SEGMENTADOR ######################################

###################################### FUNCIONES VARIAS ######################################

def allowed_file(filename: str) -> bool:
    ALLOWED_EXTS = {"xlsx"}
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTS

def excel_columns_from_bytes(data: bytes) -> List[str]:
    df = pd.read_excel(io.BytesIO(data), nrows=0)
    return df.columns.tolist()

def latest_excel_key(campana: Optional[str] = None) -> Optional[str]:
    if campana is None:
        campana = session.get("campana", "bancoppel")
    prefix = f"raw/{campana}/motor_gestion/blaster/"
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    latest = max(
        (o for o in resp.get("Contents", []) if o["Key"].lower().endswith(".xlsx")),
        key=lambda o: o["LastModified"],
        default=None,
    )
    return latest["Key"] if latest else None

def excel_columns_from_key(key: str) -> List[str]:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_excel(buf, nrows=0)
    return df.columns.tolist()

def _latest_report(campana: Optional[str] = None,bucket: str | None = None) -> Tuple[str | None, str | None, bytes | None]:
    if campana is None:                          
        campana = session.get("campana", "bancoppel")
    if bucket is None:                           
        bucket = os.environ["S3_BUCKET"]
    yyyy_mm        = datetime.today().strftime("%Y_%m")
    env_var_name   = f"REPORT_PREFIX_{campana.upper()}"      
    try:
        template   = os.environ[env_var_name]             
    except KeyError:
        raise ValueError(f"Variable de entorno '{env_var_name}' no definida")
    prefix = template.format(campana=campana, yyyy_mm=yyyy_mm)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:               
        return None, None, None
    latest = max(response["Contents"], key=lambda o: o["LastModified"])
    key    = latest["Key"]
    name   = key.rsplit("/", 1)[-1]
    body   = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return name, key, body

############################################################################

@app.route("/segmentador")
@login_required
def segmentador():
    from segmentador import status_to_dictamen as s2d
    clean_map = {k if k is not None else "": v for k, v in s2d.items() if k is not None}
    return render_template(
        "segmentador.html", page="segmentador", status_to_dictamen=clean_map
    )

############################################################################

@app.route("/segmentador/preview")
@login_required
def segmentador_preview():
    name, _, body = _latest_report()
    if not name:
        return jsonify(report_name=None), 404

    df = pd.read_csv(BytesIO(body))
    rows, cols = df.shape
    campanas = sorted(df["campaña"].dropna().unique().tolist())
    columnas = df.columns.tolist()

    return jsonify(
        report_name=name,
        row_count=rows,
        col_count=cols,
        campanas=campanas,
        columnas=columnas,
    )

############################################################################

@app.route("/segmentador/descargar")
@login_required
def segmentador_descargar():
    name, _, body = _latest_report()
    if not name:
        abort(404)
    return send_file(
        BytesIO(body),
        mimetype="text/csv",
        download_name=name,
        as_attachment=True,
    )

############################################################################

@app.route("/segmentador/filtrar")
@login_required
def segmentador_filtrar():
    estatus = request.args.get("estatus") or None
    dictamen = request.args.get("dictamen") or None
    campana = request.args.get("campana") or None
    name, _, body = _latest_report()
    if not name:
        return jsonify(error="no-report"), 404
    df = pd.read_csv(BytesIO(body))
    subset = segmentar(df, estatus, dictamen, campana)
    rows, cols = subset.shape
    preview_html = (
        subset.head(100)
        .to_html(
            index=False,
            classes="preview-table min-w-full text-xs",
            border=0,
            escape=False,
        )
    )
    stem = name.rsplit(".", 1)[0]
    subset_name = f"{stem}_filtrado.xlsx"
    return jsonify(
        report_name=name,
        subset_name=subset_name,
        row_count=rows,
        col_count=cols,
        preview_html=preview_html,
    )

############################################################################

@app.route("/segmentador/descargar_filtrado")
@login_required
def segmentador_descargar_filtrado():
    estatus = request.args.get("estatus") or None
    dictamen = request.args.get("dictamen") or None
    campana = request.args.get("campana") or None
    cols_str = request.args.get("columns")
    name, _, body = _latest_report()
    if not name:
        abort(404)
    df = pd.read_csv(BytesIO(body))
    subset = segmentar(df, estatus, dictamen, campana)
    if cols_str:
        cols_sel = [c for c in cols_str.split(",") if c in subset.columns]
        subset = subset[cols_sel]
    buff = BytesIO()
    with pd.ExcelWriter(buff, engine="xlsxwriter") as writer:
        subset.to_excel(writer, index=False, sheet_name="Datos")
    buff.seek(0)
    stem = name.rsplit(".", 1)[0]
    return send_file(
        buff,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        download_name=f"{stem}_filtrado.xlsx",
        as_attachment=True,
    )

###################################### 2.2. DETONADOR ######################################

@app.route("/detonador")
@login_required
def detonador():
    return render_template("detonador.html", page="detonador")

###################################### 2.2.1. ARA ######################################

@app.route("/detonador/ara")
@login_required
def detonador_ara():
    return render_template("detonador_ara.html", page="detonador_ara")

###################################### 2.2.2. ARIA ######################################

@app.route("/detonador/aria")
@login_required
def detonador_aria():
    return render_template("detonador_aria.html", page="detonador_aria")

###################################### 2.2.3. SAM ######################################

@app.route("/detonador/sam")
@login_required
def detonador_sam():
    return render_template("detonador_sam.html", page="detonador_sam")

###################################### 3. ORQUESTACION INTELIGENTE ###################################### 

###################################### 3.1. MOTOR DE GESTIÓN ######################################

@app.route("/motordegestion")
@login_required
def motor_gestion():
    campana      = session.get("campana")           
    cli_cfg      = CLIENTS.get(session["client_slug"], {})
    json_slugs   = cli_cfg.get("config_slugs", [])  
    configs: list[dict] = []                        
    for slug in json_slugs:
        key = f"config/{campana}/{slug}"
        try:
            obj  = s3.get_object(Bucket=S3_BUCKET_CONFIG, Key=key)
            data = json.loads(obj["Body"].read())
            configs.append({"slug": slug, "data": data})
        except s3.exceptions.NoSuchKey:
            flash(f"No se encontró {slug} en el bucket", "warning")
        except Exception as err:
            flash(f"Error leyendo {slug}: {err}", "error")
    return render_template(
        "motor_gestion.html",
        page="motor_gestion",
        configs=configs,               
    )

###################################### 

EDITABLE_KEYS = {
    "CONTACT_LIMIT_PER_ITERATION",         # PARAMETROS GENERALES
    "DIAS_HACIA_ATRAS_CONTACTO_ADMISIBLE", # PARAMETROS GENERALES
    "UNREACHABLE_DICTAMINATION_DAYS",      # PARAMETROS GENERALES
    "UNREACHABLE_DICTAMINATION_HOURS",     # PARAMETROS GENERALES
    "QUERY_TO_CONTACT",                    # FUENTE DE DATOS
    "COLUMNA_CREDITO",                     # FUENTE DE DATOS
    "COLUMNA_NOMBRE",                      # FUENTE DE DATOS
    "COLUMNA_SALDO",                       # FUENTE DE DATOS
    "COLUMNAS_TELEFONOS",                  # FUENTE DE DATOS
    "COLUMNA_QUERY",                       # FUENTE DE DATOS
    "CONTACT_CHANNELS_ORDERED",            # ESTRATEGIA DE CANALES
    "CONTACT_CHANNELS_DISTRIBUTION",       # ESTRATEGIA DE CANALES
    "CALL_AGENT_ID",                       # ESTRATEGIA DE CANALES
    "CALL_TYPE",                           # ESTRATEGIA DE CANALES
    "WA_SERVERS_AVAILABLE",                # ESTRATEGIA DE CANALES
    "WA_MESSAGE",                          # ESTRATEGIA DE CANALES
    "PERCENTAJE_OF_SEND_CALLS",            # ESTRATEGIA DE CANALES
    "PERCENTAJE_OF_SEND_WA",               # ESTRATEGIA DE CANALES
    "PERCENTAJE_OF_SEND_BLASTER",          # ESTRATEGIA DE CANALES
}

###################################### 

@app.route("/motordegestion/columns")
@login_required
def motor_gestion_columns():
    from io import BytesIO
    name, _, body = _latest_report()          
    if not name:
        return jsonify(error="no-report"), 404
    import pandas as pd
    cols = pd.read_csv(BytesIO(body), nrows=0).columns.tolist()
    return jsonify(columns=cols)

###################################### 

def _s3_json(key: str) -> dict:
    obj = s3.get_object(Bucket=S3_BUCKET_CONFIG, Key=key)
    return json.loads(obj["Body"].read())

###################################### 

@app.route("/motordegestion/audios")
@login_required
def motor_gestion_audios():
    campana = session.get("campana")
    prefix  = f"raw/{campana}/motor_gestion/blaster/audio/"
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    audios = sorted(
        os.path.splitext(o["Key"].rsplit("/", 1)[-1])[0]
        for o in resp.get("Contents", [])
    )
    return jsonify(audios=audios)

###################################### 

@app.route("/motordegestion/update_config", methods=["POST"])
@login_required
def motor_gestion_update_config():
    payload = request.get_json(silent=True) or {}
    slug    = payload.get("slug")
    upd     = payload.get("config", {})
    if not slug:
        return jsonify(error="slug requerido"), 400
    campana = session.get("campana")
    key     = f"config/{campana}/{slug}"
    try:
        cfg = _s3_json(key)
    except Exception as e:
        return jsonify(error=f"No se pudo leer configuración: {e}"), 500
    for k, v in upd.items():
        if k in EDITABLE_KEYS:
            cfg[k] = v
        elif k == "BLASTER_CONFIG_UPDATES":
            audio = v.get("nombre_del_audio")
            if audio:
                cfg.setdefault("BLASTER_CONFIG_UPDATES", {})["nombre_del_audio"] = audio
    try:
        s3.put_object(
            Bucket      = S3_BUCKET_CONFIG,
            Key         = key,
            Body        = json.dumps(cfg, ensure_ascii=False, indent=4).encode("utf-8"),
            ContentType = "application/json",
        )
    except Exception as e:
        return jsonify(error=f"No se pudo guardar: {e}"), 500
    return jsonify(ok=True)

###################################### 4. MONITOREO ######################################

@app.route("/monitoreo")
@login_required
def monitoreo():
    return render_template("monitoreo.html", page="monitoreo")

###################################### CONTACTO ######################################

@app.route("/contacto")
@login_required
def contacto():
    return render_template("contacto.html", page="contacto")

###################################### LOGS ######################################

@app.route("/stream-logs/<job_id>")
@login_required
def stream_logs(job_id: str):
    q = LOG_QUEUES.get(job_id)
    if q is None:
        return "job_id desconocido", 404
    def gen():
        while True:
            line = q.get()
            if line is None:
                yield "data: __END__\n\n"
                break
            yield f"data: {line.rstrip()}\n\n"
    return Response(gen(), mimetype="text/event-stream")

############################################# PÁGINAS ESTÁTICAS #############################################

@app.route("/<string:pagina>")
@login_required
def pagina_estatica(pagina):
    try:
        return render_template(f"{pagina}.html", page=pagina)
    except TemplateNotFound:
        abort(404)
        
@app.errorhandler(404)
def not_found(e):
    return "Página no encontrada", 404

############################################# EJECUTADOR #############################################
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)