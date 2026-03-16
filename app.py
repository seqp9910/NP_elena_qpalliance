#!/usr/bin/env python3
"""
Elena NP — Backend Flask
Pipeline de Notificaciones Personales para QPAlliance
"""
import os, re, json, uuid, threading, datetime, shutil, subprocess
import unicodedata, base64, zipfile, io, sys, time
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_file, Response

# ─── APP SETUP ───────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

BASE_DIR   = Path(__file__).parent
ASSETS_DIR = BASE_DIR / 'assets'
JOBS_DIR   = BASE_DIR / 'jobs'
JOBS_DIR.mkdir(exist_ok=True)

JOBS: dict = {}

BREVO_API_KEY = os.environ.get('BREVO_API_KEY', '')
SENDER_NAME   = 'Notificaciones Judiciales - QPAlliance'
SENDER_EMAIL  = 'notificacionesjudiciales@qpalliance.co'

# ─── UTILS ───────────────────────────────────────────────────────────────────
def hoy_str():
    m = ['','enero','febrero','marzo','abril','mayo','junio',
         'julio','agosto','septiembre','octubre','noviembre','diciembre']
    t = datetime.date.today()
    return f"{t.day:02d} de {m[t.month]} de {t.year}"

def normalizar(s: str) -> str:
    """Lowercase + strip accents."""
    s = s.strip().lower()
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )

def parse_codigos(raw: str) -> list:
    """
    Parse codes from semicolon/comma/space/newline separated string.
    Accepts: '1372;1496'  OR  'R1372;R1496'  OR  'r1372, r1496'
    Returns list of unique ints.
    """
    result = []
    for part in re.split(r'[;,\n\r\t ]+', raw.strip()):
        part = part.strip()
        if not part:
            continue
        m = re.match(r'^[Rr]?(\d+)$', part)
        if m:
            result.append(int(m.group(1)))
    return list(dict.fromkeys(result))  # deduplicate preserving order

def find_pdf_for_code(code: int, paths: list) -> Path | None:
    """Find a PDF whose filename contains the code number."""
    code_str = str(code)
    # Priority: R0*1372 pattern
    for p in paths:
        if re.search(r'[Rr]0*' + code_str + r'[\W_\.]', p.name + '.'):
            return p
    # Fallback: code digits anywhere in stem
    for p in paths:
        if code_str in re.sub(r'\D', '', p.stem):
            return p
    return None

def load_excel(excel_path: Path) -> dict:
    """
    Load the NP base Excel file.
    Supports 'Nueva base radicacion de demandas.xlsx' which has 3 sheets:
      Numeros, Listas, Total
    Reads the 'Total' sheet (or first sheet if not found).
    Columns expected: Cod, #, Abog, Nombre, Ciudad, Juzgado, Radicado,
                      Fecha_Demanda, Tipo de proceso, Jurisdiccion, etc.
    Returns dict: {code_int: {'Nombre':..., 'Radicado':..., 'Ciudad':...,
                               'Juzgado':..., 'fecha_admite':...}}
    """
    import pandas as pd
    import openpyxl

    # 1. Detect sheet names
    try:
        wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
        sheets = wb.sheetnames
        wb.close()
    except Exception:
        sheets = []

    # 2. Prefer 'Total' sheet, then first
    if 'Total' in sheets:
        sheet_name = 'Total'
    elif sheets:
        sheet_name = sheets[0]
    else:
        sheet_name = 0  # pandas default

    # 3. Read
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=0)

    # 4. Normalize column names for mapping
    # Priority for num column: '#' exact > 'numero'/'num' > 'cod'/'codigo'
    col_map = {}
    for col in df.columns:
        norm = normalizar(str(col))
        # '#' gets highest priority as the sequential case number
        if norm == '#':
            col_map['num'] = col          # override always — '#' wins
        elif norm in ('num', 'numero', 'n', 'no') and 'num' not in col_map:
            col_map['num'] = col
        elif norm in ('cod', 'codigo') and 'num' not in col_map:
            col_map.setdefault('num', col)
        elif 'nombre' in norm:
            col_map.setdefault('nombre', col)
        elif 'radicado' in norm:
            col_map.setdefault('radicado', col)
        elif 'ciudad' in norm:
            col_map.setdefault('ciudad', col)
        elif 'juzgado' in norm:
            col_map.setdefault('juzgado', col)
        elif 'fecha' in norm and 'admite' in norm:
            col_map['fecha_admite'] = col      # prefer fecha_admite over fecha_demanda
        elif 'fecha' in norm and 'demanda' in norm:
            col_map.setdefault('fecha_admite', col)
        elif 'correo' in norm or 'email' in norm or 'electroni' in norm or 'direcc' in norm:
            col_map.setdefault('email', col)

    # 5. Build lookup dict
    result = {}
    num_col = col_map.get('num')
    if num_col is None:
        # Last resort: try '#' directly
        if '#' in df.columns:
            num_col = '#'
        else:
            raise ValueError(
                f"No se encontro columna de codigo en hoja '{sheet_name}'. "
                f"Columnas disponibles: {list(df.columns)}"
            )

    for _, row in df.iterrows():
        try:
            raw_code = row[num_col]
            if pd.isna(raw_code):
                continue
            # Accept integer or string like 'R1372'
            code_str = str(raw_code).strip()
            m = re.match(r'^[Rr]?(\d+)', code_str)
            if not m:
                continue
            code = int(m.group(1))
        except Exception:
            continue

        # fecha_admite
        fa_col = col_map.get('fecha_admite')
        fa_raw = row.get(fa_col) if fa_col else None
        if fa_raw is not None and not (isinstance(fa_raw, float) and pd.isna(fa_raw)):
            try:
                if isinstance(fa_raw, (datetime.datetime, datetime.date)):
                    fecha_admite = fa_raw.strftime('%d/%m/%Y')
                else:
                    fecha_admite = str(fa_raw).strip()
            except Exception:
                fecha_admite = str(fa_raw).strip()
        else:
            fecha_admite = ''

        def get_col(key, default=''):
            col = col_map.get(key)
            if not col:
                return default
            val = row.get(col, default)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return str(val).strip()

        result[code] = {
            'Nombre':       get_col('nombre'),
            'Radicado':     get_col('radicado'),
            'Ciudad':       get_col('ciudad'),
            'Juzgado':      get_col('juzgado'),
            'fecha_admite': fecha_admite,
            'email':        get_col('email'),
        }

    return result

def fill_template(template_path: Path, data: dict, output_path: Path):
    """Replace {placeholder} in DOCX template and save to output_path."""
    import shutil as _shutil
    import zipfile as _zipfile

    _shutil.copy2(template_path, output_path)

    with _zipfile.ZipFile(output_path, 'r') as z:
        names = z.namelist()
        contents = {n: z.read(n) for n in names}

    fecha_hoy = hoy_str()

    def replace_in_xml(xml_bytes: bytes) -> bytes:
        text = xml_bytes.decode('utf-8')
        for key, val in data.items():
            text = text.replace('{' + key + '}', str(val))
            text = text.replace('{ ' + key + '}', str(val))  # space variant
            text = text.replace('{' + key + ' }', str(val))
        text = text.replace('{fecha_de_hoy}', fecha_hoy)
        text = text.replace('{ fecha_de_hoy}', fecha_hoy)
        return text.encode('utf-8')

    xml_files = [n for n in names if n.endswith('.xml') or n.endswith('.rels')]

    with _zipfile.ZipFile(output_path, 'w', _zipfile.ZIP_DEFLATED) as zout:
        for name in names:
            if name in xml_files:
                zout.writestr(name, replace_in_xml(contents[name]))
            else:
                zout.writestr(name, contents[name])

def docx_to_pdf(docx_path: Path, output_dir: Path) -> Path:
    """Convert DOCX to PDF using LibreOffice headless."""
    result = subprocess.run(
        ['libreoffice', '--headless', '--convert-to', 'pdf',
         '--outdir', str(output_dir), str(docx_path)],
        capture_output=True, text=True, timeout=60
    )
    pdf_name = docx_path.stem + '.pdf'
    pdf_path = output_dir / pdf_name
    if not pdf_path.exists():
        raise RuntimeError(
            f"LibreOffice fallo convirtiendo {docx_path.name}: {result.stderr}"
        )
    return pdf_path

def merge_pdfs(pdf_list: list, output_path: Path):
    """Merge list of PDF paths into a single PDF using pypdf."""
    from pypdf import PdfWriter
    writer = PdfWriter()
    for p in pdf_list:
        writer.append(str(p))
    with open(output_path, 'wb') as f:
        writer.write(f)

def build_receipt_pdf(codes_data: list, output_path: Path):
    """Build a receipt PDF listing all processed cases."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(output_path), pagesize=letter)
    elements = []

    elements.append(Paragraph("Constancia de Notificaciones Personales", styles['Title']))
    elements.append(Paragraph(f"Fecha: {hoy_str()}", styles['Normal']))
    elements.append(Spacer(1, 12))

    data = [['Codigo', 'Nombre', 'Radicado', 'Ciudad', 'Juzgado']]
    for row in codes_data:
        data.append([
            str(row.get('code', '')),
            row.get('Nombre', ''),
            row.get('Radicado', ''),
            row.get('Ciudad', ''),
            row.get('Juzgado', ''),
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#C4006A')),
        ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
        ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0, 0), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f0f5')]),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING',  (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(table)
    doc.build(elements)

def send_email_brevo(to_email: str, to_name: str, subject: str,
                     html_body: str, attachment_path: Path = None):
    """Send email via Brevo transactional API."""
    import urllib.request
    if not BREVO_API_KEY:
        return False, "BREVO_API_KEY no configurada"

    payload = {
        "sender":  {"name": SENDER_NAME, "email": SENDER_EMAIL},
        "to":      [{"email": to_email, "name": to_name}],
        "subject": subject,
        "htmlContent": html_body,
    }

    if attachment_path and attachment_path.exists():
        with open(attachment_path, 'rb') as f:
            content_b64 = base64.b64encode(f.read()).decode()
        payload["attachment"] = [{
            "content": content_b64,
            "name": attachment_path.name,
        }]

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        'https://api.brevo.com/v3/smtp/email',
        data=data,
        headers={
            'Content-Type': 'application/json',
            'api-key': BREVO_API_KEY,
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return True, resp.read().decode()
    except Exception as e:
        return False, str(e)

# ─── JOB RUNNER ──────────────────────────────────────────────────────────────
def run_job(job_id: str, job_dir: Path, codigos: list,
            excel_path: Path, autos_pdfs: list, demandas_pdfs: list,
            dest_email: str):
    """Background thread: full NP pipeline for one batch."""
    job = JOBS[job_id]

    def log(msg: str, step: int = None):
        job['log'].append(msg)
        if step is not None:
            job['step'] = step
        print(f"[{job_id}] {msg}")

    try:
        job['status'] = 'running'
        template_path = ASSETS_DIR / 'modelo_NP.docx'

        # STEP 1 — Load Excel
        log("Cargando base de datos Excel...", step=1)
        excel_data = load_excel(excel_path)
        sample_keys = list(excel_data.keys())[:5]
        log(f"   OK: {len(excel_data)} registros cargados. Primeros codigos: {sample_keys}")
        log(f"   Codigos solicitados: {codigos}")

        # Verify all requested codes exist
        found    = [c for c in codigos if c in excel_data]
        missing  = [c for c in codigos if c not in excel_data]
        log(f"   Encontrados: {found} | No encontrados: {missing}")
        if not found:
            log("   ERROR: Ninguno de los codigos solicitados existe en el Excel. "
                "Verifica que estas subiendo la hoja 'Total' y usando los numeros de la columna '#'.")
            job['status'] = 'error'
            job['error']  = f"Codigos {codigos} no encontrados. Primeros en Excel: {sample_keys}"
            return

        # STEP 2 — Fill DOCX templates and convert to PDF
        log("Generando autos admisorios...", step=2)
        generated_pdfs = []
        cases_info = []

        for code in codigos:
            if code not in excel_data:
                log(f"   Codigo {code} no encontrado en Excel, omitiendo.")
                continue

            row = excel_data[code]
            fill_data = {
                'Nombre':       row.get('Nombre', ''),
                'Radicado':     row.get('Radicado', ''),
                'Ciudad':       row.get('Ciudad', ''),
                'Juzgado':      row.get('Juzgado', ''),
                'fecha_admite': row.get('fecha_admite', ''),
                'fecha_de_hoy': hoy_str(),
            }

            docx_out = job_dir / f"auto_{code}.docx"
            fill_template(template_path, fill_data, docx_out)

            try:
                pdf_out = docx_to_pdf(docx_out, job_dir)
                generated_pdfs.append(pdf_out)
                log(f"   OK auto admisorio: R{code} - {row.get('Nombre','')}")
            except Exception as e:
                log(f"   Error convirtiendo R{code}: {e}")

            cases_info.append({'code': code, **row})

        # STEP 3 — Merge PDFs (auto + autos_pdf + demanda_pdf per case)
        log("Ensamblando paquetes PDF...", step=3)
        paquetes = []

        for code in codigos:
            if code not in excel_data:
                continue

            auto_pdf        = next((p for p in generated_pdfs
                                    if p.stem == f'auto_{code}'), None)
            auto_pdf_source = find_pdf_for_code(code, autos_pdfs)
            demanda_pdf     = find_pdf_for_code(code, demandas_pdfs)

            parts = []
            if auto_pdf and auto_pdf.exists():
                parts.append(auto_pdf)
            elif auto_pdf_source:
                parts.append(auto_pdf_source)

            if demanda_pdf:
                parts.append(demanda_pdf)

            if not parts:
                log(f"   Sin PDFs para R{code}, omitiendo paquete.")
                continue

            paquete_path = job_dir / f"NP_R{code}.pdf"
            try:
                merge_pdfs(parts, paquete_path)
                paquetes.append(paquete_path)
                log(f"   OK paquete R{code}: {len(parts)} archivo(s) merged")
            except Exception as e:
                log(f"   Error ensamblando R{code}: {e}")

        # STEP 4 — Build constancia/receipt
        log("Generando constancia...", step=4)
        receipt_path = job_dir / 'constancia_NP.pdf'
        try:
            build_receipt_pdf(cases_info, receipt_path)
            log("   OK constancia generada.")
        except Exception as e:
            log(f"   Error generando constancia: {e}")
            receipt_path = None

        # STEP 5 — ZIP everything
        log("Empaquetando archivos finales...", step=5)
        zip_path = job_dir / f'NP_lote_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for p in paquetes:
                zf.write(p, p.name)
            if receipt_path and receipt_path.exists():
                zf.write(receipt_path, receipt_path.name)

        log(f"   OK ZIP creado: {zip_path.name} ({zip_path.stat().st_size // 1024} KB)")

        # STEP 6 — Send email
        if dest_email and BREVO_API_KEY:
            log(f"Enviando notificacion a {dest_email}...", step=6)
            html = f"""
            <h2>Lote NP procesado - QPAlliance</h2>
            <p>Se procesaron <strong>{len(paquetes)}</strong> notificacion(es) personal(es).</p>
            <p>Fecha: {hoy_str()}</p>
            <p>Adjunto encontrara el ZIP con todos los paquetes PDF.</p>
            """
            ok, msg = send_email_brevo(
                dest_email, dest_email,
                f"Lote NP - {len(paquetes)} notificaciones - {hoy_str()}",
                html, zip_path
            )
            if ok:
                log("   OK email enviado correctamente.")
            else:
                log(f"   Email fallido: {msg}")
        elif dest_email:
            log("   Email no enviado (BREVO_API_KEY no configurada).")

        job['status']   = 'done'
        job['zip_path'] = str(zip_path)
        job['paquetes'] = len(paquetes)
        job['total']    = len(codigos)
        job['cases']    = cases_info
        log("Pipeline completado.", step=6)

    except Exception as e:
        import traceback
        job['status'] = 'error'
        job['error']  = str(e)
        log(f"Error fatal: {e}\n{traceback.format_exc()}")

# ─── ROUTES ──────────────────────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    # 1. Parse codes
    raw_codigos = request.form.get('codigos', '').strip()
    if not raw_codigos:
        return jsonify({'error': 'No se ingresaron codigos.'}), 400

    codigos = parse_codigos(raw_codigos)
    if not codigos:
        return jsonify({
            'error': 'No se ingresaron codigos validos. '
                     'Ingrese numeros separados por ";" (ej: 1372;1496)'
        }), 400

    # 2. Save uploaded files
    job_id  = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True)

    # Excel base — acepta 'excel' o 'base_file' (nombre que usa el HTML)
    excel_file = request.files.get('excel') or request.files.get('base_file')
    if not excel_file or not excel_file.filename:
        return jsonify({'error': 'Debe subir el archivo Excel de base.'}), 400

    excel_path = job_dir / 'base.xlsx'
    excel_file.save(str(excel_path))

    # Autos PDFs — acepta 'autos_pdfs' o 'autos'
    autos_files = request.files.getlist('autos_pdfs') or request.files.getlist('autos')
    autos_pdfs  = []
    autos_dir   = job_dir / 'autos'
    autos_dir.mkdir()
    for f in autos_files:
        if f and f.filename:
            p = autos_dir / f.filename
            f.save(str(p))
            autos_pdfs.append(p)

    # Demandas PDFs — acepta 'demandas_pdfs' o 'demandas'
    demandas_files = request.files.getlist('demandas_pdfs') or request.files.getlist('demandas')
    demandas_pdfs  = []
    demandas_dir   = job_dir / 'demandas'
    demandas_dir.mkdir()
    for f in demandas_files:
        if f and f.filename:
            p = demandas_dir / f.filename
            f.save(str(p))
            demandas_pdfs.append(p)

    # Destination email — acepta 'email' o 'email_to'
    dest_email = (request.form.get('email') or request.form.get('email_to') or '').strip()

    # 3. Register job
    JOBS[job_id] = {
        'status':  'queued',
        'step':    0,
        'log':     [],
        'codigos': codigos,
    }

    # 4. Launch background thread
    t = threading.Thread(
        target=run_job,
        args=(job_id, job_dir, codigos, excel_path,
              autos_pdfs, demandas_pdfs, dest_email),
        daemon=True
    )
    t.start()

    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({'error': 'Job no encontrado'}), 404
    return jsonify({
        'status':   job['status'],
        'step':     job.get('step', 0),
        'log':      job.get('log', []),
        'paquetes': job.get('paquetes', 0),
        'total':    job.get('total', len(job.get('codigos', []))),
        'error':    job.get('error', ''),
    })

@app.route('/download/<job_id>')
def download(job_id):
    job = JOBS.get(job_id)
    if not job or job['status'] != 'done':
        return jsonify({'error': 'Job no listo'}), 404
    zip_path = Path(job['zip_path'])
    if not zip_path.exists():
        return jsonify({'error': 'Archivo no encontrado'}), 404
    return send_file(str(zip_path), as_attachment=True,
                     download_name=zip_path.name)

@app.route('/debug-excel', methods=['GET', 'POST'])
def debug_excel():
    """Upload an Excel and get a JSON diagnostic: sheets, columns, sample codes."""
    if request.method == 'GET':
        return '''<!DOCTYPE html><html><head><title>Debug Excel</title>
        <style>body{font-family:sans-serif;max-width:600px;margin:80px auto;background:#0a0a0a;color:#fff}
        input,button{margin:10px 0;padding:10px;font-size:16px}button{background:#D4006A;color:#fff;border:none;cursor:pointer;border-radius:6px}
        pre{background:#1a1a1a;padding:15px;border-radius:8px;overflow-x:auto;font-size:13px;max-height:500px;overflow-y:auto}</style></head>
        <body><h2>Debug Excel — Elena NP</h2>
        <form method="POST" enctype="multipart/form-data">
        <p>Sube tu Excel de base de radicación:</p>
        <input type="file" name="base_file" accept=".xlsx,.xls" required><br>
        <button type="submit">Analizar</button></form></body></html>'''
    f = request.files.get('excel') or request.files.get('base_file')
    if not f:
        return jsonify({'error': 'Sube el Excel con campo "excel"'}), 400
    import tempfile, pandas as pd, openpyxl
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        f.save(tmp.name)
        tmp_path = Path(tmp.name)
    try:
        wb = openpyxl.load_workbook(tmp_path, read_only=True, data_only=True)
        sheets = wb.sheetnames
        wb.close()
        sheet_name = 'Total' if 'Total' in sheets else sheets[0]
        df = pd.read_excel(tmp_path, sheet_name=sheet_name, header=0)
        cols = list(df.columns)
        # Find # column
        num_col = '#' if '#' in df.columns else cols[0]
        sample = []
        for _, row in df.head(10).iterrows():
            val = row[num_col]
            sample.append(str(val))
        return jsonify({
            'sheets': sheets,
            'sheet_used': sheet_name,
            'columns': [str(c) for c in cols],
            'num_col_detected': num_col,
            'sample_codes_from_num_col': sample,
            'total_rows': len(df),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        tmp_path.unlink(missing_ok=True)

@app.route('/dashboard/<job_id>')
def dashboard(job_id):
    job = JOBS.get(job_id)
    if not job:
        return "Job no encontrado", 404

    cases    = job.get('cases', [])
    paquetes = job.get('paquetes', 0)
    total    = job.get('total', 0)
    log_lines = job.get('log', [])

    rows_html = ''
    for c in cases:
        rows_html += f"""
        <tr>
          <td>R{c.get('code','')}</td>
          <td>{c.get('Nombre','')}</td>
          <td>{c.get('Radicado','')}</td>
          <td>{c.get('Ciudad','')}</td>
          <td>{c.get('Juzgado','')}</td>
          <td>{c.get('fecha_admite','')}</td>
        </tr>"""

    log_html = '\n'.join(
        f'<div class="log-line">{line}</div>'
        for line in log_lines
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Dashboard NP - {job_id[:8]}</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; background:#0f0f1a; color:#e0e0e0;
         margin:0; padding:24px; }}
  h1 {{ color:#e91e8c; }}
  .stat-row {{ display:flex; gap:24px; margin:16px 0; }}
  .stat {{ background:#1a1a2e; border-radius:12px; padding:16px 24px; min-width:120px; }}
  .stat .num {{ font-size:2rem; font-weight:700; color:#e91e8c; }}
  .stat .lbl {{ font-size:.85rem; color:#888; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; margin-top:16px; }}
  th {{ background:#e91e8c; color:#fff; padding:8px 12px; text-align:left; }}
  td {{ padding:7px 12px; border-bottom:1px solid #2a2a3e; }}
  tr:hover td {{ background:#1a1a2e; }}
  .log {{ background:#0a0a14; border-radius:8px; padding:12px; margin-top:24px;
          max-height:300px; overflow-y:auto; font-size:.82rem; }}
  .log-line {{ padding:3px 0; border-bottom:1px solid #1a1a2e; }}
  .btn {{ background:#e91e8c; color:#fff; border:none; border-radius:8px;
          padding:10px 24px; font-size:1rem; cursor:pointer; text-decoration:none;
          display:inline-block; margin-top:16px; }}
  .btn:hover {{ background:#c4006a; }}
</style>
</head>
<body>
<h1>Dashboard - Lote NP</h1>
<div class="stat-row">
  <div class="stat"><div class="num">{paquetes}</div><div class="lbl">Paquetes generados</div></div>
  <div class="stat"><div class="num">{total}</div><div class="lbl">Codigos procesados</div></div>
  <div class="stat"><div class="num">{hoy_str()}</div><div class="lbl">Fecha</div></div>
</div>
<a href="/download/{job_id}" class="btn">Descargar ZIP</a>
<table>
  <thead>
    <tr><th>Codigo</th><th>Nombre</th><th>Radicado</th>
        <th>Ciudad</th><th>Juzgado</th><th>Fecha admision</th></tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
<div class="log">{log_html}</div>
</body>
</html>"""

# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
