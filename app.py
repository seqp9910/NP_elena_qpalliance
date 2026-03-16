#!/usr/bin/env python3
"""
Elena NP — Backend Flask
Pipeline de Notificaciones Personales para QPAlliance
"""
import os, sys, re, json, uuid, threading, datetime, shutil, subprocess
import unicodedata, base64, time, zipfile, io
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_file, Response

# ─── APP SETUP ──────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

BASE_DIR   = Path(__file__).parent
ASSETS_DIR = BASE_DIR / 'assets'
JOBS_DIR   = BASE_DIR / 'jobs'
JOBS_DIR.mkdir(exist_ok=True)

JOBS: dict = {}

# ─── BREVO CONFIG ────────────────────────────────────────────────────────────
BREVO_API_KEY  = os.environ.get('BREVO_API_KEY', '')
SENDER_NAME    = 'Notificaciones Judiciales - QPAlliance'
SENDER_EMAIL   = 'notificacionesjudiciales@qpalliance.co'

# ─── HELPERS ─────────────────────────────────────────────────────────────────
meses = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
         'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}

def normalizar(s):
    """Remove accents and lowercase for fuzzy matching."""
    if not s:
        return ''
    s = str(s)
    return ''.join(c for c in unicodedata.normalize('NFD', s.lower())
                   if unicodedata.category(c) != 'Mn')

def parse_codigos(raw: str) -> list:
    """Parse codes from semicolon/comma/newline separated string.
    Accepts both R1372 and 1372 formats. Returns list of int codes."""
    codigos = []
    for part in re.split(r'[;,\n\r]+', raw):
        part = part.strip()
        if not part:
            continue
        # Strip leading R/r
        m = re.match(r'^[Rr]?(\d+)$', part)
        if m:
            codigos.append(int(m.group(1)))
    return list(dict.fromkeys(codigos))  # deduplicate preserving order

def hoy_str():
    today = datetime.date.today()
    meses_es = ['','enero','febrero','marzo','abril','mayo','junio',
                'julio','agosto','septiembre','octubre','noviembre','diciembre']
    return f"{today.day:02d} de {meses_es[today.month]} de {today.year}"

def find_pdf_for_code(code: int, pdf_paths: list) -> Path | None:
    """Find a PDF file whose name contains the code number."""
    code_str = str(code)
    # First try: exact match with R prefix pattern
    for p in pdf_paths:
        if re.search(r'[Rr]' + code_str + r'[\W_]', p.name):
            return p
    # Second try: code number anywhere in filename
    for p in pdf_paths:
        if code_str in p.stem:
            return p
    return None

# ─── EXCEL LOADER ────────────────────────────────────────────────────────────
def load_excel(excel_path: Path) -> dict:
    """Load Excel base file. Returns dict keyed by int code."""
    import pandas as pd
    # Try reading with header detection
    df = None
    for header_row in [0, 1, 2, 3, 4]:
        try:
            tmp = pd.read_excel(excel_path, header=header_row)
            cols_norm = [normalizar(str(c)) for c in tmp.columns]
            if any(c in cols_norm for c in ['#', 'num', 'numero', 'codigo', 'cod']):
                df = tmp
                break
            if any('nombre' in c or 'radicado' in c for c in cols_norm):
                df = tmp
                break
        except Exception:
            continue

    if df is None:
        df = pd.read_excel(excel_path)

    # Normalize column names
    col_map = {}
    for col in df.columns:
        norm = normalizar(str(col))
        if norm in ('#', 'num', 'numero', 'n', 'cod', 'codigo'):
            col_map['num'] = col
        elif 'nombre' in norm:
            col_map['nombre'] = col
        elif 'radicado' in norm:
            col_map['radicado'] = col
        elif 'ciudad' in norm:
            col_map['ciudad'] = col
        elif 'juzgado' in norm:
            col_map['juzgado'] = col
        elif 'fecha_admite' in norm or ('fecha' in norm and 'admit' in norm):
            col_map['fecha_admite'] = col
        elif 'fecha' in norm and 'demand' in norm:
            col_map['fecha_demanda'] = col

    result = {}
    for _, row in df.iterrows():
        # Get code
        raw_num = row.get(col_map.get('num', '#'), None) if col_map.get('num') else None
        if raw_num is None or str(raw_num).strip() in ('', 'nan', 'NaN'):
            continue
        try:
            m = re.match(r'^[Rr]?(\d+)', str(raw_num).strip())
            if not m:
                continue
            code = int(m.group(1))
        except (ValueError, TypeError):
            continue

        def safe(key):
            col = col_map.get(key)
            if col is None:
                return ''
            val = row.get(col, '')
            return '' if str(val).lower() in ('nan', 'none', '') else str(val).strip()

        result[code] = {
            'num':          code,
            'Nombre':       safe('nombre'),
            'Radicado':     safe('radicado') or f'R{code}',
            'Ciudad':       safe('ciudad') or 'Bogotá',
            'Juzgado':      safe('juzgado') or '',
            'fecha_admite': safe('fecha_admite'),
            'fecha_demanda':safe('fecha_demanda'),
        }
    return result

# ─── DOCX GENERATION ─────────────────────────────────────────────────────────
def generar_np_pdf(code: int, client: dict, job_dir: Path) -> Path:
    """Fill modelo_NP.docx with client data and convert to PDF."""
    import zipfile, shutil

    modelo = ASSETS_DIR / 'modelo_NP.docx'
    docx_out = job_dir / f'R{code}.NP.docx'
    pdf_out  = job_dir / f'R{code}.NP.pdf'

    # Read docx XML and replace placeholders
    with zipfile.ZipFile(modelo, 'r') as zin:
        with zipfile.ZipFile(docx_out, 'w', zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == 'word/document.xml':
                    text = data.decode('utf-8')
                    replacements = {
                        '{Nombre}':        client.get('Nombre', ''),
                        '{ Nombre}':       client.get('Nombre', ''),
                        '{Radicado}':      client.get('Radicado', f"R{code}"),
                        '{ Radicado}':     client.get('Radicado', f"R{code}"),
                        '{Ciudad}':        client.get('Ciudad', 'Bogotá'),
                        '{ Ciudad}':       client.get('Ciudad', 'Bogotá'),
                        '{Juzgado}':       client.get('Juzgado', ''),
                        '{ Juzgado}':      client.get('Juzgado', ''),
                        '{fecha_admite}':  client.get('fecha_admite', ''),
                        '{ fecha_admite}': client.get('fecha_admite', ''),
                        '{fecha_de_hoy}':  hoy_str(),
                        '{ fecha_de_hoy}': hoy_str(),
                    }
                    for old, new in replacements.items():
                        text = text.replace(old, new)
                    data = text.encode('utf-8')
                zout.writestr(item, data)

    # Convert to PDF with LibreOffice
    result = subprocess.run(
        ['libreoffice', '--headless', '--convert-to', 'pdf',
         '--outdir', str(job_dir), str(docx_out)],
        capture_output=True, timeout=120
    )
    if result.returncode != 0 or not pdf_out.exists():
        raise RuntimeError(f"LibreOffice falló: {result.stderr.decode()[:200]}")

    docx_out.unlink(missing_ok=True)
    return pdf_out

# ─── PDF MERGE ───────────────────────────────────────────────────────────────
def merge_pdfs(paths: list, output: Path):
    """Merge list of PDF paths into output."""
    from pypdf import PdfWriter
    writer = PdfWriter()
    for p in paths:
        writer.append(str(p))
    with open(output, 'wb') as f:
        writer.write(f)

# ─── EMAIL RECEIPT ───────────────────────────────────────────────────────────
def generar_recibo_pdf(code: int, client: dict, subject: str,
                        sent_dt: str, msg_id: str, size_mb: float,
                        output: Path):
    """Generate a PDF receipt of the sent email."""
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.units import cm

    doc = SimpleDocTemplate(str(output), pagesize=letter,
                            leftMargin=3*cm, rightMargin=3*cm,
                            topMargin=2.5*cm, bottomMargin=2.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'],
                                  fontSize=14, textColor=colors.HexColor('#D4006A'))
    body_style  = ParagraphStyle('body',  parent=styles['Normal'],
                                  fontSize=10, leading=16)
    label_style = ParagraphStyle('label', parent=styles['Normal'],
                                  fontSize=9, textColor=colors.grey)

    story = [
        Paragraph("Constancia de Notificación Personal", title_style),
        HRFlowable(width="100%", thickness=1, color=colors.HexColor('#D4006A')),
        Spacer(1, 0.4*cm),
        Paragraph(f"<b>Radicado:</b> R{code} — {client.get('Nombre','')}", body_style),
        Paragraph(f"<b>Juzgado:</b> {client.get('Juzgado','')}", body_style),
        Paragraph(f"<b>Enviado:</b> {sent_dt}", body_style),
        Paragraph(f"<b>Asunto:</b> {subject}", body_style),
        Paragraph(f"<b>ID Mensaje:</b> {str(msg_id)[:60]}", body_style),
        Paragraph(f"<b>Tamaño adjunto:</b> {size_mb} MB", body_style),
        Spacer(1, 0.5*cm),
        Paragraph("QPAlliance — notificacionesjudiciales@qpalliance.co", label_style),
    ]
    doc.build(story)

# ─── BREVO EMAIL ─────────────────────────────────────────────────────────────
def send_email_brevo(code: int, client: dict, pdf_path: Path,
                      to_email: str, cc_email: str = '') -> tuple:
    """Send email via Brevo API. Returns (subject, filename, size_mb, msg_id)."""
    import requests as req

    nombre   = client.get('Nombre', f'Cliente R{code}')
    radicado = client.get('Radicado', f'R{code}')
    fecha_admite = client.get('fecha_admite', '')

    subject  = f"R{code} Notificación personal - {radicado} - {nombre}"
    filename = pdf_path.name
    size_mb  = round(pdf_path.stat().st_size / 1_000_000, 1)

    with open(pdf_path, 'rb') as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    body_html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#222;padding:20px">
<p>Señores,<br>Rappi S.A.S.<br>Felipe Villamarín Lafaurie</p>
<p><strong>RADICADO:</strong> {radicado}<br>
<strong>REFERENCIA:</strong> Demanda ordinaria laboral promovida por {nombre} en contra de Rappi SAS<br>
<strong>ASUNTO:</strong> Notificación personal de auto admisorio de demanda ordinaria laboral de primera instancia.</p>
<p>Reciban un cordial saludo. De manera atenta, conforme lo dispuesto por el artículo 8 de la Ley 2213 de 2022,
nos permitimos notificarle el auto del {fecha_admite}, por medio del cual se admite la demanda que impetra nuestro
cliente, {nombre}.</p>
<p>A la presente se adjunta:</p>
<ol>
  <li>Auto admisorio de la demanda.</li>
  <li>Notificación personal.</li>
  <li>Escrito de demanda.</li>
  <li>Prueba 1.1.1 (video)</li>
  <li>Pruebas documentales 1.1.2. a 1.1.16.</li>
  <li>Poder debidamente otorgado</li>
  <li>Anexos: Certificado de existencia y representación legal de la firma de abogados QPALLIANCE SAS,
      certificado de existencia y representación legal de Rappi SAS</li>
  <li>Proyecto de liquidación de pretensiones.</li>
</ol>
<br>
<p style="color:#888;font-size:9pt;border-top:2px solid #D4006A;padding-top:8px">
Legal Department | QPAlliance<br>
notificacionesjudiciales@qpalliance.co | www.qpalliance.co
</p>
</body></html>"""

    payload = {
        "sender":      {"name": SENDER_NAME, "email": SENDER_EMAIL},
        "to":          [{"email": to_email}],
        "subject":     subject,
        "htmlContent": body_html,
        "attachment":  [{"content": pdf_b64, "name": filename}],
    }
    if cc_email:
        payload["cc"] = [{"email": cc_email}]

    resp = req.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    msg_id = resp.json().get('messageId', '')
    return subject, filename, size_mb, msg_id

# ─── DASHBOARD ───────────────────────────────────────────────────────────────
def generar_dashboard_html(results: list, lote_id: str) -> str:
    total    = len(results)
    enviados = sum(1 for r in results if r.get('enviado'))
    errores  = total - enviados

    filas = ""
    for r in results:
        st = "✅" if r.get('enviado') else "❌"
        filas += f"""<tr>
          <td>R{r['code']}</td>
          <td>{r.get('nombre','')}</td>
          <td>{r.get('radicado','')}</td>
          <td style="text-align:center">{st}</td>
          <td>{r.get('msg','')}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"/>
<title>Dashboard NP — {lote_id}</title>
<style>
  body{{font-family:Arial,sans-serif;background:#f5f5f5;padding:20px;color:#222}}
  h2{{color:#D4006A}}
  .stats{{display:flex;gap:20px;margin-bottom:20px}}
  .stat{{background:#fff;border-radius:8px;padding:16px 24px;box-shadow:0 1px 4px rgba(0,0,0,.1);min-width:120px}}
  .stat .num{{font-size:2rem;font-weight:700;color:#D4006A}}
  .stat .lbl{{font-size:.8rem;color:#888}}
  table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.1)}}
  th{{background:#D4006A;color:#fff;padding:10px 12px;text-align:left;font-size:.85rem}}
  td{{padding:9px 12px;font-size:.85rem;border-bottom:1px solid #f0f0f0}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#fdf0f5}}
</style>
</head>
<body>
<h2>📊 Dashboard — Lote NP {lote_id}</h2>
<div class="stats">
  <div class="stat"><div class="num">{total}</div><div class="lbl">Total</div></div>
  <div class="stat"><div class="num" style="color:#4caf50">{enviados}</div><div class="lbl">Enviados</div></div>
  <div class="stat"><div class="num" style="color:#f44336">{errores}</div><div class="lbl">Errores</div></div>
</div>
<table>
  <thead><tr>
    <th>Código</th><th>Nombre</th><th>Radicado</th><th>Estado</th><th>Detalle</th>
  </tr></thead>
  <tbody>{filas}</tbody>
</table>
</body></html>"""

# ─── PIPELINE BACKGROUND ─────────────────────────────────────────────────────
def run_pipeline(job_id: str, codigos: list, excel_path: Path,
                  auto_paths: list, demanda_paths: list,
                  to_email: str, cc_email: str):
    job = JOBS[job_id]
    job['status']   = 'running'
    job['progress'] = 0
    job['messages'] = []
    job['sent']     = 0
    job['total']    = len(codigos)

    def log(msg, pct=None):
        job['messages'].append(msg)
        if pct is not None:
            job['progress'] = pct

    job_dir  = JOBS_DIR / job_id
    done_dir = job_dir / 'done'
    job_dir.mkdir(exist_ok=True)
    done_dir.mkdir(exist_ok=True)

    try:
        # 1. Load Excel
        log("📊 Cargando base de radicación...", 5)
        try:
            clientes = load_excel(excel_path)
        except Exception as e:
            raise RuntimeError(f"Error leyendo Excel: {e}")

        log(f"   → {len(clientes)} registros cargados del Excel", 10)

        results = []
        n = len(codigos)

        for i, code in enumerate(codigos):
            pct_base = 10 + int((i / n) * 75)
            log(f"\n── R{code}", pct_base)

            client = clientes.get(code)
            if not client:
                log(f"   ✗ R{code} no encontrado en Excel (se omite)")
                results.append({'code': code, 'nombre': '', 'radicado': f'R{code}',
                                 'enviado': False, 'msg': 'No encontrado en Excel'})
                continue

            nombre   = client.get('Nombre') or f'Cliente R{code}'
            radicado = client.get('Radicado') or f'R{code}'
            log(f"   Nombre: {nombre}")

            # Find auto PDF
            auto_pdf = find_pdf_for_code(code, auto_paths)
            if not auto_pdf:
                log(f"   ✗ Auto admisorio no encontrado para R{code} (se omite)")
                results.append({'code': code, 'nombre': nombre, 'radicado': radicado,
                                 'enviado': False, 'msg': 'Auto no encontrado'})
                continue

            # Find demanda PDF (optional)
            demanda_pdf = find_pdf_for_code(code, demanda_paths)

            try:
                # 2. Generate NP PDF
                log(f"   Generando NP...")
                np_pdf = generar_np_pdf(code, client, job_dir)
                log(f"   ✓ NP generado")

                # 3. Merge: auto + NP + demanda
                to_merge = [auto_pdf, np_pdf]
                if demanda_pdf:
                    to_merge.append(demanda_pdf)
                merged_pdf = job_dir / f'R{code}.DDD.NP.pdf'
                merge_pdfs(to_merge, merged_pdf)
                size_mb = round(merged_pdf.stat().st_size / 1_000_000, 1)
                log(f"   ✓ PDF fusionado ({size_mb} MB)")

                # 4. Send email
                if BREVO_API_KEY:
                    log(f"   Enviando a {to_email}...")
                    subj, fname, sz, msg_id = send_email_brevo(
                        code, client, merged_pdf, to_email, cc_email)
                    sent_dt = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                    log(f"   ✓ Enviado ({sz} MB) — {str(msg_id)[:40]}")

                    # 5. Generate receipt
                    receipt_pdf = job_dir / f'print_R{code}.pdf'
                    generar_recibo_pdf(code, client, subj, sent_dt,
                                       msg_id, sz, receipt_pdf)

                    # 6. Done PDF = merged + receipt
                    done_pdf = done_dir / f'R{code}.DDD.NP.done.pdf'
                    merge_pdfs([merged_pdf, receipt_pdf], done_pdf)
                    receipt_pdf.unlink(missing_ok=True)
                    pages = 0
                    try:
                        from pypdf import PdfReader
                        pages = len(PdfReader(str(done_pdf)).pages)
                    except Exception:
                        pass
                    log(f"   ✓ {done_pdf.name} ({pages} págs)")
                    job['sent'] += 1
                    results.append({'code': code, 'nombre': nombre, 'radicado': radicado,
                                     'enviado': True, 'msg': f'Enviado {sz}MB'})
                else:
                    # No API key — just save merged PDF
                    done_pdf = done_dir / f'R{code}.DDD.NP.pdf'
                    shutil.copy(merged_pdf, done_pdf)
                    log(f"   ⚠ Sin API key Brevo — PDF guardado sin enviar")
                    results.append({'code': code, 'nombre': nombre, 'radicado': radicado,
                                     'enviado': False, 'msg': 'Sin clave Brevo'})

            except Exception as e:
                log(f"   ✗ Error R{code}: {e}")
                results.append({'code': code, 'nombre': nombre, 'radicado': radicado,
                                 'enviado': False, 'msg': str(e)[:80]})

        # 7. Create ZIP
        log("\n📦 Creando ZIP...", 88)
        zip_path = job_dir / f'NP_{job_id[:8]}.zip'
        pdf_files = list(done_dir.glob('*.pdf'))
        if not pdf_files:
            pdf_files = list(job_dir.glob('*.DDD.NP.pdf'))

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(pdf_files):
                zf.write(p, p.name)
        log(f"   ✓ ZIP con {len(pdf_files)} archivos")

        # 8. Dashboard
        log("📊 Generando dashboard...", 95)
        lote_id = datetime.date.today().strftime('%d-%m-%Y')
        dash_html = generar_dashboard_html(results, lote_id)
        dash_path = job_dir / 'dashboard.html'
        with open(dash_path, 'w', encoding='utf-8') as f:
            f.write(dash_html)

        log(f"\n✅ Completado — {job['sent']}/{n} enviados", 100)
        job.update({
            'status':    'done',
            'zip_path':  str(zip_path),
            'dash_path': str(dash_path),
            'results':   results,
        })

    except Exception as e:
        job['status'] = 'error'
        job['error']  = str(e)
        job['messages'].append(f"❌ Error fatal: {e}")
    finally:
        # Cleanup uploaded files
        try:
            shutil.rmtree(job_dir / 'uploads', ignore_errors=True)
        except Exception:
            pass

# ─── ROUTES ──────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/process', methods=['POST'])
def process():
    # Parse codes
    codigos_raw = request.form.get('codigos', '').strip()
    codigos = parse_codigos(codigos_raw)
    if not codigos:
        return jsonify({'error': 'No se ingresaron códigos válidos. '
                        'Usa formato: 1372;1496;1563 o R1372;R1496;R1563'}), 400

    # Excel
    base_file = request.files.get('base_file')
    if not base_file:
        return jsonify({'error': 'No se subió el archivo Excel de base de radicación'}), 400

    # Autos
    autos = request.files.getlist('autos')
    if not autos or all(f.filename == '' for f in autos):
        return jsonify({'error': 'No se subieron los PDFs de autos admisorios'}), 400

    # Demandas
    demandas = request.files.getlist('demandas')

    # Save uploads to disk
    job_id  = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    up_dir  = job_dir / 'uploads'
    up_dir.mkdir(parents=True, exist_ok=True)

    excel_path = up_dir / base_file.filename
    base_file.save(str(excel_path))

    auto_paths = []
    for f in autos:
        if f.filename:
            p = up_dir / f.filename
            f.save(str(p))
            auto_paths.append(p)

    demanda_paths = []
    for f in demandas:
        if f.filename:
            p = up_dir / f.filename
            f.save(str(p))
            demanda_paths.append(p)

    to_email = request.form.get('email_to', '').strip()
    cc_email = request.form.get('email_cc', '').strip()

    JOBS[job_id] = {
        'status': 'queued', 'progress': 0, 'messages': [],
        'sent': 0, 'total': len(codigos),
    }

    t = threading.Thread(
        target=run_pipeline,
        args=(job_id, codigos, excel_path, auto_paths, demanda_paths, to_email, cc_email),
        daemon=True
    )
    t.start()

    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)

@app.route('/download/<job_id>')
def download(job_id):
    job = JOBS.get(job_id)
    if not job or job.get('status') != 'done':
        return jsonify({'error': 'Not ready'}), 404
    zip_path = job.get('zip_path')
    if not zip_path or not Path(zip_path).exists():
        return jsonify({'error': 'ZIP not found'}), 404
    return send_file(zip_path, as_attachment=True,
                     download_name=Path(zip_path).name)

@app.route('/dashboard/<job_id>')
def dashboard(job_id):
    job = JOBS.get(job_id)
    if not job:
        return "Job not found", 404
    dash_path = job.get('dash_path')
    if not dash_path or not Path(dash_path).exists():
        return "<html><body><p>Dashboard no disponible</p></body></html>"
    with open(dash_path, 'r', encoding='utf-8') as f:
        return f.read()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
