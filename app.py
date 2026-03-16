#!/usr/bin/env python3
"""
Elena NP — Backend Flask
Pipeline de Notificaciones Personales para QPAlliance
"""
import os, sys, re, json, uuid, threading, datetime, shutil, subprocess
import unicodedata, base64, time, zipfile, io
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_file, Response, stream_with_context

# ─── APP SETUP ──────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max

BASE_DIR   = Path(__file__).parent
ASSETS_DIR = BASE_DIR / 'assets'
JOBS_DIR   = BASE_DIR / 'jobs'
JOBS_DIR.mkdir(exist_ok=True)

# In-memory job tracking
JOBS: dict[str, dict] = {}

# ─── BREVO CONFIG ────────────────────────────────────────────────────────────
BREVO_API_KEY  = os.environ.get('BREVO_API_KEY', '')
SENDER_NAME    = 'Notificaciones Judiciales - QPAlliance'
SENDER_EMAIL   = 'notificacionesjudiciales@qpalliance.co'

# ─── HELPERS ────────────────────────────────────────────────────────────────
meses = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
         'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}
mes_list   = '|'.join(meses.keys())
meses_es   = ['','enero','febrero','marzo','abril','mayo','junio','julio',
              'agosto','septiembre','octubre','noviembre','diciembre']
num_words  = {'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,
              'nueve':9,'diez':10,'once':11,'doce':12,'trece':13,'catorce':14,'quince':15,
              'dieciséis':16,'dieciseis':16,'diecisiete':17,'dieciocho':18,'diecinueve':19,
              'veinte':20,'veintiuno':21,'veintiún':21,'veintidós':22,'veintidos':22,
              'veintitrés':23,'veintitres':23,'veinticuatro':24,'veinticinco':25,
              'veintiséis':26,'veintiseis':26,'veintisiete':27,'veintiocho':28,'veintinueve':29,'treinta':30}
year_words = {'veinticuatro':2024,'veinticinco':2025,'veintiséis':2026,'veintiseis':2026,'veintisiete':2027}

def normalize(s):
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn').lower().strip()

def extract_date(text):
    m = re.search(r'(\w+)\s+\w+\s+\((\d{1,2})\)\s+de\s+.*?\((\d{4})\)', text, re.IGNORECASE)
    if m and m.group(1).lower() in meses:
        return f"{int(m.group(2)):02d} de {m.group(1).lower()} de {m.group(3)}"
    m = re.search(r'\((\d{1,2})\)\s+de\s+(\w+)\s+de\s+.*?\((\d{4})\)', text, re.IGNORECASE)
    if m and m.group(2).lower() in meses:
        return f"{int(m.group(1)):02d} de {m.group(2).lower()} de {m.group(3)}"
    m = re.search(rf'\b(\d{{1,2}})\s+de\s+({mes_list})\s+de\s+(\d{{4}})\b', text, re.IGNORECASE)
    if m: return f"{int(m.group(1)):02d} de {m.group(2).lower()} de {m.group(3)}"
    cities = r'(?:medell[íi]n|bogot[áa]|cali|barranquilla|bucaramanga|manizales|pereira|armenia|ibagu[ée]|c[úu]cuta|cartagena|santa\s*marta|popay[áa]n|monter[ií]a|villavicencio|neiva|tunja|pasto)'
    m2 = re.search(rf'{cities}[,.\s]+(\w+)\s+(?:\(\d+\)\s+)?de\s+(\w+)\s+de\s+dos\s+mil\s+(\w+)', text, re.IGNORECASE)
    if m2:
        d = num_words.get(m2.group(1).lower()); mes = m2.group(2).lower(); yr = year_words.get(m2.group(3).lower())
        if d and mes in meses and yr: return f"{d:02d} de {mes} de {yr}"
    return None

def parse_date(s):
    import pandas as pd
    if not s or (isinstance(s, float) and pd.isna(s)): return None
    s = str(s)
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
    if m: return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    m = re.match(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', s)
    if m and m.group(2).lower() in meses:
        return datetime.date(int(m.group(3)), meses[m.group(2).lower()], int(m.group(1)))
    return None

def log(job_id, msg, level='info'):
    if job_id in JOBS:
        JOBS[job_id]['logs'].append({'level': level, 'msg': msg, 'ts': datetime.datetime.now().strftime('%H:%M:%S')})
    print(f"[{job_id[:8]}] {msg}")

# ─── EMAIL HTML ──────────────────────────────────────────────────────────────
def build_email_html(nombre, radicado, fecha_admite, logo_b64):
    return f"""<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#222;margin:0;padding:20px">
<p>Señores,<br>Rappi S.A.S.<br>Felipe Villamarín Lafaurie</p>
<p><strong>RADICADO:</strong> {radicado}<br>
<strong>REFERENCIA:</strong> Demanda ordinaria laboral promovida por {nombre} en contra de Rappi SAS<br>
<strong>ASUNTO:</strong> Notificación personal de auto admisorio de demanda ordinaria laboral de primera instancia.</p>
<p>Reciban un cordial saludo. De manera atenta, conforme lo dispuesto por el artículo 8 de la Ley 2213
de 2022, nos permitimos notificarle el auto del {fecha_admite}, por medio del cual se admite la demanda
que impetra nuestro cliente, {nombre}.</p>
<p>A la presente se adjunta:</p>
<ol>
  <li>Auto admisorio de la demanda.</li><li>Notificación personal.</li><li>Escrito de demanda.</li>
  <li>Prueba 1.1.1 (video)</li><li>Pruebas documentales 1.1.2. a 1.1.16.</li>
  <li>Poder debidamente otorgado</li>
  <li>Anexos: Certificado de existencia y representación legal de la firma de abogados QPALLIANCE SAS,
      certificado de existencia y representación legal de Rappi SAS</li>
  <li>Proyecto de liquidación de pretensiones.</li>
</ol>
<br>
<table cellpadding="0" cellspacing="0" border="0" style="border-top:2px solid #D4006A;padding-top:10px;margin-top:8px;font-family:Calibri,Arial,sans-serif;font-size:10pt;color:#444">
  <tr>
    <td style="padding-right:14px;vertical-align:middle">
      <img src="data:image/png;base64,{logo_b64}" height="65" alt="QPAlliance" style="display:block">
    </td>
    <td style="vertical-align:middle">
      <div style="font-weight:700;font-size:11pt;color:#111;margin-bottom:5px">Legal Department | QPAlliance</div>
      <div style="line-height:1.9">
        <a href="https://www.qpalliance.co" style="color:#444;text-decoration:none">🌐 www.qpalliance.co</a><br>
        <a href="mailto:notificacionesjudiciales@qpalliance.co" style="color:#444;text-decoration:none">✉ notificacionesjudiciales@qpalliance.co</a>
      </div>
    </td>
  </tr>
</table>
</body></html>"""

# ─── PIPELINE ───────────────────────────────────────────────────────────────
def run_pipeline(job_id, job_dir, codes, to_email, cc_email, send_emails):
    import pandas as pd
    from openpyxl import load_workbook
    from pypdf import PdfReader, PdfWriter
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import cm

    try:
        import requests as req_lib
    except:
        req_lib = None

    try:
        pdfmetrics.registerFont(TTFont('Caladea-Bold',
            '/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf'))
        FONT = 'Caladea-Bold'
    except:
        FONT = 'Helvetica-Bold'

    logo_b64 = (ASSETS_DIR / 'logo_b64.txt').read_text().strip()
    today    = datetime.date.today()
    errors   = []

    try:
        JOBS[job_id]['status'] = 'running'

        # ── 1. LEER BASE Y CRUZAR ──────────────────────────────────────────
        log(job_id, '📋 Paso 1/6 — Cruzando códigos con base de radicación...')
        JOBS[job_id]['step'] = 1

        df_base = pd.read_excel(str(job_dir / 'base_radicacion.xlsx'), sheet_name='Total')
        df_base['#'] = pd.to_numeric(df_base['#'], errors='coerce')

        clients = {}
        for code in codes:
            rows = df_base[df_base['#'] == code]
            if rows.empty:
                errors.append(f"Código {code}: no encontrado en base de radicación")
                log(job_id, f'  ⚠️  #{code}: no encontrado en base', 'warn')
                continue
            row = rows.iloc[0]
            fd = row.get('Fecha_Demanda', '')
            if pd.isna(fd): fd = ''
            elif hasattr(fd, 'strftime'): fd = fd.strftime('%d/%m/%Y')
            else: fd = str(fd).strip()
            clients[code] = {
                '#': code, 'Nombre': str(row.get('Nombre','')).strip(),
                'Ciudad': str(row.get('Ciudad','')).strip(),
                'Juzgado': str(row.get('Juzgado','')).strip(),
                'Radicado': str(row.get('Radicado','')).strip(),
                'Fecha_Demanda': fd, 'fecha_admite': ''
            }
            log(job_id, f'  ✅ #{code}: {clients[code]["Nombre"]}')

        # ── 2. MAPEAR AUTOS Y FECHAS ────────────────────────────────────────
        log(job_id, '📄 Paso 2/6 — Extrayendo fechas de autos admisorios...')
        JOBS[job_id]['step'] = 2

        autos_dir = job_dir / 'autos'
        auto_files = [f for f in os.listdir(autos_dir) if f.lower().endswith('.pdf')]
        auto_texts = {}
        for af in auto_files:
            r = subprocess.run(['pdftotext', str(autos_dir / af), '-'], capture_output=True, text=True)
            auto_texts[af] = r.stdout

        auto_map = {}
        for code, client in clients.items():
            rad = client['Radicado'].replace('-','').replace(' ','')
            rad_prefix = rad[:16]
            nombre_words = [w for w in normalize(client['Nombre']).split() if len(w) > 2]
            best_score, best_auto = 0, None
            for af, text in auto_texts.items():
                tn = normalize(text).replace('-','').replace(' ','')
                score = 0
                if rad in tn: score += 100
                elif rad_prefix in tn: score += 80
                for w in nombre_words:
                    if w in normalize(text): score += 10
                if score > best_score:
                    best_score = score; best_auto = af
            if best_score >= 30:
                auto_map[code] = best_auto
                log(job_id, f'  ✅ #{code} → {best_auto}')
            else:
                errors.append(f"Código {code}: sin auto admisorio coincidente")
                log(job_id, f'  ⚠️  #{code}: sin auto', 'warn')

        codes_with_date = []
        for code in auto_map:
            date = extract_date(auto_texts[auto_map[code]])
            if date:
                clients[code]['fecha_admite'] = date
                codes_with_date.append(code)
                log(job_id, f'  📅 #{code}: {date}')
            else:
                errors.append(f"Código {code}: no se pudo extraer fecha del auto")

        # ── 3. GENERAR PDFs NOTIFICACIÓN ────────────────────────────────────
        log(job_id, '📝 Paso 3/6 — Generando documentos de notificación...')
        JOBS[job_id]['step'] = 3

        tmp_dir = job_dir / '_tmp'
        tmp_dir.mkdir(exist_ok=True)
        template_dir = tmp_dir / 'template_unpacked'

        # Find soffice scripts
        xlsx_scripts = Path('/usr/local/lib/python3.11/site-packages')
        soffice_found = False
        for candidate in [
            Path('/sessions/focused-keen-knuth/mnt/.skills/skills/xlsx/scripts'),
        ]:
            if candidate.exists():
                sys.path.insert(0, str(candidate))
                try:
                    from office.soffice import run_soffice
                    PACK   = str(candidate / 'office/pack.py')
                    UNPACK = str(candidate / 'office/unpack.py')
                    soffice_found = True
                    break
                except: pass

        if not soffice_found:
            def run_soffice(args, **kw):
                return subprocess.run(['libreoffice'] + args, **kw)
            PACK   = None
            UNPACK = None

        template_docx = ASSETS_DIR / 'modelo_NP.docx'
        if template_dir.exists(): shutil.rmtree(template_dir)

        if UNPACK:
            subprocess.run(['python3', UNPACK, str(template_docx), str(template_dir)], capture_output=True)
        else:
            subprocess.run(['unzip', '-q', str(template_docx), '-d', str(template_dir)])

        with open(template_dir / 'word/document.xml', 'r', encoding='utf-8') as f:
            template_xml = f.read()

        fecha_hoy = f"{today.day:02d} de {meses_es[today.month]} de {today.year}"
        np_pdfs = {}

        for code in codes_with_date:
            cl = clients[code]
            tmp_doc = tmp_dir / f'doc_{code}'
            if tmp_doc.exists(): shutil.rmtree(tmp_doc)
            shutil.copytree(template_dir, tmp_doc)
            xml = template_xml
            xml = xml.replace('{Juzgado}', cl['Juzgado'])
            xml = xml.replace('{Nombre}', cl['Nombre'])
            xml = xml.replace('{fecha_de_hoy}', fecha_hoy)
            xml = xml.replace('{fecha_admite}', cl['fecha_admite'])
            xml = xml.replace('<w:t xml:space="preserve">        {</w:t>',
                              f'<w:t xml:space="preserve">        {cl["Radicado"]}</w:t>')
            xml = xml.replace('<w:t>Radicado}</w:t>', '<w:t></w:t>')
            xml = xml.replace('<w:t>En {</w:t>', f'<w:t>En {cl["Ciudad"]}</w:t>')
            xml = xml.replace('<w:t xml:space="preserve">Ciudad}, </w:t>', '<w:t xml:space="preserve">, </w:t>')
            with open(tmp_doc / 'word/document.xml', 'w', encoding='utf-8') as f: f.write(xml)

            docx_out = tmp_dir / f'R{code}_NP.docx'
            if PACK:
                subprocess.run(['python3', PACK, str(tmp_doc), str(docx_out)], capture_output=True)
            else:
                subprocess.run(['bash','-c',f'cd "{tmp_doc}" && zip -qr "{docx_out}" .'])

            run_soffice(['--headless','--convert-to','pdf','--outdir', str(tmp_dir), str(docx_out)],
                        timeout=90, capture_output=True)

            np_pdf = tmp_dir / f'R{code}_NP.pdf'
            if np_pdf.exists():
                np_pdfs[code] = np_pdf
                log(job_id, f'  ✅ #{code}: notificación generada')
            else:
                errors.append(f"Código {code}: error generando PDF notificación")
                log(job_id, f'  ❌ #{code}: error generando PDF', 'error')
            shutil.rmtree(tmp_doc, ignore_errors=True)
            if docx_out.exists(): docx_out.unlink()

        # ── 4. MAPEAR DEMANDAS Y MERGE ──────────────────────────────────────
        log(job_id, '🔗 Paso 4/6 — Fusionando PDFs (NP + Demanda)...')
        JOBS[job_id]['step'] = 4

        demandas_dir = job_dir / 'demandas'
        demanda_map  = {}
        for df_name in os.listdir(demandas_dir):
            if not df_name.lower().endswith('.pdf'): continue
            m = re.search(r'R(\d+)DDD', df_name)
            if m: demanda_map[int(m.group(1))] = df_name

        out_dir = job_dir / f'NP_{today.strftime("%d-%m-%y")}'
        out_dir.mkdir(exist_ok=True)

        sep_path = tmp_dir / 'sep_demanda.pdf'
        c = rl_canvas.Canvas(str(sep_path), pagesize=letter)
        w, h = letter
        c.setFont(FONT, 70)
        txt = "DEMANDA"
        c.drawString((w - c.stringWidth(txt, FONT, 70))/2, h/2, txt)
        c.save()

        final_count = 0
        for code in codes_with_date:
            if code not in np_pdfs: continue
            if code not in demanda_map:
                errors.append(f"Código {code}: sin demanda (nombre debe contener R{code}DDD)")
                shutil.copy2(np_pdfs[code], out_dir / f'R{code}_Notificacion_personal.pdf')
                log(job_id, f'  ⚠️  #{code}: solo notificación (sin demanda)', 'warn')
                continue
            writer = PdfWriter()
            for page in PdfReader(str(np_pdfs[code])).pages:                writer.add_page(page)
            for page in PdfReader(str(sep_path)).pages:                      writer.add_page(page)
            dem = PdfReader(str(demandas_dir / demanda_map[code]))
            for page in dem.pages: writer.add_page(page)
            final_pdf = out_dir / f'R{code}.DDD.NP.pdf'
            with open(final_pdf, 'wb') as f: writer.write(f)
            final_count += 1
            log(job_id, f'  ✅ #{code}: R{code}.DDD.NP.pdf generado')

        for p in np_pdfs.values():
            if p.exists(): p.unlink()
        if sep_path.exists(): sep_path.unlink()

        # ── 5. DASHBOARD ─────────────────────────────────────────────────────
        log(job_id, '📊 Paso 5/6 — Generando dashboard...')
        JOBS[job_id]['step'] = 5

        city_counts, city_days, table_rows = {}, {}, []
        for code in codes:
            if code not in clients: continue
            cl = clients[code]
            ciudad = cl['Ciudad']
            fd = parse_date(cl['Fecha_Demanda']); fa = parse_date(cl['fecha_admite'])
            has_full = (out_dir / f'R{code}.DDD.NP.pdf').exists()
            has_np   = (out_dir / f'R{code}_Notificacion_personal.pdf').exists()
            if has_full: status, sc = 'Completo', 'ok';       city_counts[ciudad] = city_counts.get(ciudad,0)+1
            elif has_np: status, sc = 'Solo NP', 'warn';      city_counts[ciudad] = city_counts.get(ciudad,0)+1
            else:        status, sc = 'Sin archivos', 'error'
            days = ''
            if fd and fa:
                diff = (fa-fd).days; days = str(diff)
                if has_full or has_np: city_days.setdefault(ciudad,[]).append(diff)
            table_rows.append(
                f'<tr class="{sc}"><td>{code}</td><td>{cl["Nombre"]}</td><td>{ciudad}</td>'
                f'<td>{cl["Juzgado"]}</td><td style="font-size:0.75em">{cl["Radicado"]}</td>'
                f'<td>{cl["Fecha_Demanda"]}</td><td>{cl["fecha_admite"]}</td><td>{days}</td>'
                f'<td><span class="badge {sc}">{status}</span></td></tr>'
            )

        city_avg = {c:round(sum(d)/len(d),1) for c,d in city_days.items()}
        cs  = sorted(city_counts.keys()); cd  = [city_counts[c] for c in cs]
        acs = sorted(city_avg.keys());   ad  = [city_avg[c]    for c in acs]
        all_d   = [d for ds in city_days.values() for d in ds]
        avg_all = round(sum(all_d)/len(all_d),1) if all_d else 0

        dash_html = f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Dashboard NP – {today.strftime('%d/%m/%y')}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>*{{margin:0;padding:0;box-sizing:border-box}}body{{background:#0a0a0a;color:#e0e0e0;font-family:'Segoe UI',system-ui,sans-serif}}
.header{{background:linear-gradient(135deg,#1a0011,#2a0020,#0a0a0a);padding:30px 40px;display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #D4006A}}
.logo-text{{font-size:2em;font-weight:900;color:#D4006A;letter-spacing:-1px}}.header-right{{text-align:right}}
.header-right h1{{font-size:1.4em;color:#D4006A}}.header-right p{{color:#888;font-size:0.85em;margin-top:4px}}
.container{{max-width:1400px;margin:0 auto;padding:30px}}
.kpi-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:20px;margin-bottom:30px}}
.kpi{{background:#141414;border:1px solid #2a2a2a;border-radius:12px;padding:24px;text-align:center}}
.kpi .value{{font-size:2.5em;font-weight:700;color:#D4006A}}.kpi .label{{font-size:0.85em;color:#888;margin-top:8px}}
.charts-row{{display:grid;grid-template-columns:1fr 1fr;gap:30px;margin-bottom:30px}}
.chart-box{{background:#141414;border:1px solid #2a2a2a;border-radius:12px;padding:24px}}
.chart-box h3{{color:#D4006A;margin-bottom:16px}}
.table-section{{background:#141414;border:1px solid #2a2a2a;border-radius:12px;padding:24px;overflow-x:auto}}
.table-section h3{{color:#D4006A;margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;font-size:0.85em}}
th{{background:#1a1a1a;color:#D4006A;padding:12px 8px;text-align:left;border-bottom:2px solid #D4006A}}
td{{padding:10px 8px;border-bottom:1px solid #1f1f1f}}tr:hover{{background:#1a1a1a}}tr.error td{{opacity:0.5}}
.badge{{padding:3px 10px;border-radius:20px;font-size:0.8em;font-weight:600}}
.badge.ok{{background:#0d3320;color:#4ade80}}.badge.warn{{background:#3b3011;color:#fbbf24}}.badge.error{{background:#3b1111;color:#f87171}}
</style></head><body>
<div class="header">
  <div class="logo-text">elena</div>
  <div class="header-right"><h1>Dashboard Notificaciones Personales</h1>
  <p>Lote NP_{today.strftime('%d-%m-%y')} | {today.day:02d} de {meses_es[today.month]} de {today.year} | Empowered by QPAlliance</p></div>
</div>
<div class="container">
<div class="kpi-row">
  <div class="kpi"><div class="value">{len(codes)}</div><div class="label">Total en lote</div></div>
  <div class="kpi"><div class="value">{final_count}</div><div class="label">NPs completas</div></div>
  <div class="kpi"><div class="value">{len(codes)-final_count}</div><div class="label">Sin procesar</div></div>
  <div class="kpi"><div class="value">{len(city_counts)}</div><div class="label">Ciudades</div></div>
  <div class="kpi"><div class="value">{avg_all}</div><div class="label">Días promedio</div></div>
</div>
<div class="charts-row">
  <div class="chart-box"><h3>Notificaciones por ciudad</h3><canvas id="c1"></canvas></div>
  <div class="chart-box"><h3>Días promedio demanda → admisión</h3><canvas id="c2"></canvas></div>
</div>
<div class="table-section"><h3>Detalle ({len(codes)} demandantes)</h3>
<table><thead><tr><th>#</th><th>Nombre</th><th>Ciudad</th><th>Juzgado</th><th>Radicado</th><th>F. Demanda</th><th>F. Admite</th><th>Días</th><th>Estado</th></tr></thead>
<tbody>{''.join(table_rows)}</tbody></table></div></div>
<script>Chart.defaults.color='#888';Chart.defaults.borderColor='#2a2a2a';
new Chart(document.getElementById('c1'),{{type:'bar',data:{{labels:{json.dumps(cs)},datasets:[{{data:{json.dumps(cd)},backgroundColor:'#D4006A',borderRadius:6}}]}},options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,ticks:{{stepSize:1}}}}}}}}}});
new Chart(document.getElementById('c2'),{{type:'bar',data:{{labels:{json.dumps(acs)},datasets:[{{data:{json.dumps(ad)},backgroundColor:'#FF6B35',borderRadius:6}}]}},options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true}}}}}}}}}});</script>
</body></html>"""

        dash_path = out_dir / f'Dashboard_NP_{today.strftime("%d-%m-%y")}.html'
        dash_path.write_text(dash_html, encoding='utf-8')

        # ── 6. ENVIAR CORREOS ─────────────────────────────────────────────────
        JOBS[job_id]['step'] = 6
        done_dir = job_dir / f'NP_{today.strftime("%d-%m-%y")}-done'
        done_dir.mkdir(exist_ok=True)

        pink = colors.HexColor('#D4006A')

        if send_emails and req_lib:
            log(job_id, f'📧 Paso 6/6 — Enviando correos a {to_email}...')
            for code in codes_with_date:
                pdf_path = out_dir / f'R{code}.DDD.NP.pdf'
                if not pdf_path.exists(): continue
                cl = clients[code]
                subject = f"R{cl['#']} Notificación personal - {cl['Radicado']} - {cl['Nombre']}"
                body    = build_email_html(cl['Nombre'], cl['Radicado'], cl['fecha_admite'], logo_b64)
                with open(pdf_path, 'rb') as f:
                    pdf_b64_content = base64.b64encode(f.read()).decode()
                payload = {
                    "sender": {"name": SENDER_NAME, "email": SENDER_EMAIL},
                    "to":     [{"email": to_email}],
                    "subject": subject,
                    "htmlContent": body,
                    "attachment": [{"content": pdf_b64_content, "name": pdf_path.name}]
                }
                if cc_email:
                    payload["cc"] = [{"email": cc_email}]
                try:
                    r = req_lib.post("https://api.brevo.com/v3/smtp/email",
                        headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
                        json=payload, timeout=120)
                    r.raise_for_status()
                    msg_id = r.json().get("messageId","")

                    # Print PDF
                    styles = getSampleStyleSheet()
                    hl = ParagraphStyle('hl', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#888888'))
                    hv = ParagraphStyle('hv', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Bold')
                    bp = ParagraphStyle('bp', parent=styles['Normal'], fontSize=9, leading=14)
                    print_path = tmp_dir / f'R{code}.print.pdf'
                    doc = SimpleDocTemplate(str(print_path), pagesize=letter,
                                            topMargin=1.5*cm, bottomMargin=1.5*cm,
                                            leftMargin=2*cm, rightMargin=2*cm)
                    sent_dt = datetime.datetime.now()
                    content = [
                        Paragraph("REGISTRO DE CORREO ENVIADO",
                            ParagraphStyle('t', parent=styles['Normal'], fontSize=11,
                                           fontName='Helvetica-Bold', textColor=pink)),
                        Spacer(1,10), HRFlowable(width="100%",thickness=1,color=pink), Spacer(1,8),
                        Table([
                            [Paragraph("De:",hl),    Paragraph(f"{SENDER_NAME} &lt;{SENDER_EMAIL}&gt;",hv)],
                            [Paragraph("Para:",hl),  Paragraph(to_email,hv)],
                            [Paragraph("CC:",hl),    Paragraph(cc_email or "—",hv)],
                            [Paragraph("Fecha:",hl), Paragraph(sent_dt.strftime('%d/%m/%Y %H:%M'),hv)],
                            [Paragraph("Asunto:",hl),Paragraph(subject[:80],hv)],
                        ], colWidths=[2.5*cm, None],
                        style=[('VALIGN',(0,0),(-1,-1),'TOP'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]),
                        Spacer(1,10), HRFlowable(width="100%",thickness=0.5,color=colors.HexColor('#ddd')),
                        Spacer(1,8),
                        Paragraph(f"Notificación personal de auto admisorio. RADICADO: {cl['Radicado']}. "
                                  f"Demandante: {cl['Nombre']}. Fecha auto: {cl['fecha_admite']}.", bp),
                        Spacer(1,12), HRFlowable(width="100%",thickness=1,color=pink), Spacer(1,4),
                        Paragraph("Legal Department | QPAlliance | www.qpalliance.co",
                            ParagraphStyle('ft', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#888'))),
                    ]
                    doc.build(content)

                    # Merge → .done.pdf
                    done_pdf = done_dir / f'R{code}.DDD.NP.done.pdf'
                    writer = PdfWriter()
                    for page in PdfReader(str(pdf_path)).pages:    writer.add_page(page)
                    for page in PdfReader(str(print_path)).pages:  writer.add_page(page)
                    with open(done_pdf, 'wb') as f: writer.write(f)
                    print_path.unlink(missing_ok=True)
                    log(job_id, f'  ✅ #{code}: correo enviado + .done generado')
                except Exception as e:
                    errors.append(f"Código {code}: error al enviar correo — {e}")
                    log(job_id, f'  ❌ #{code}: {e}', 'error')
                time.sleep(0.4)
        else:
            log(job_id, '📧 Paso 6/6 — Omitiendo envío de correos (no configurado)')

        # Cleanup tmp
        shutil.rmtree(tmp_dir, ignore_errors=True)

        # Error log
        (out_dir / 'log_errores.txt').write_text('\n'.join(errors) if errors else 'Sin errores.')

        # ── ZIP DE RESULTADOS ─────────────────────────────────────────────────
        log(job_id, '📦 Empaquetando resultados...')
        zip_path = job_dir / f'NP_{today.strftime("%d-%m-%y")}.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # PDFs generados
            for f in out_dir.iterdir():
                zf.write(f, f'{out_dir.name}/{f.name}')
            # .done PDFs (si se enviaron correos)
            if done_dir.exists():
                for f in done_dir.iterdir():
                    zf.write(f, f'{done_dir.name}/{f.name}')

        JOBS[job_id].update({
            'status':      'done',
            'final_count': final_count,
            'errors':      errors,
            'zip_path':    str(zip_path),
            'dash_name':   dash_path.name,
            'out_dir_name': out_dir.name,
        })
        log(job_id, f'🎉 ¡Listo! {final_count} PDFs generados. {len(errors)} errores.')

    except Exception as e:
        import traceback
        JOBS[job_id]['status'] = 'error'
        JOBS[job_id]['error']  = str(e)
        log(job_id, f'💥 Error fatal: {e}', 'error')
        traceback.print_exc()


# ─── ROUTES ─────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    # Parse codes
    codes_raw = request.form.get('codes', '').strip()
    codes = [int(c.strip()) for c in re.split(r'[;,\n\s]+', codes_raw) if c.strip().isdigit()]
    if not codes:
        return jsonify({'error': 'No se ingresaron códigos válidos'}), 400

    to_email  = request.form.get('to_email', '').strip()
    cc_email  = request.form.get('cc_email', '').strip()
    send_flag = request.form.get('send_emails', 'true').lower() == 'true'

    if not to_email:
        return jsonify({'error': 'Correo destino requerido'}), 400

    # Create job
    job_id  = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir()
    (job_dir / 'autos').mkdir()
    (job_dir / 'demandas').mkdir()

    # Save uploaded files
    base_file = request.files.get('base_radicacion')
    if not base_file:
        return jsonify({'error': 'Falta el archivo base de radicación'}), 400
    base_file.save(str(job_dir / 'base_radicacion.xlsx'))

    autos = request.files.getlist('autos')
    for af in autos:
        if af.filename: af.save(str(job_dir / 'autos' / af.filename))

    demandas = request.files.getlist('demandas')
    for df in demandas:
        if df.filename: df.save(str(job_dir / 'demandas' / df.filename))

    JOBS[job_id] = {'status': 'queued', 'step': 0, 'logs': [], 'error': None}

    # Run in background
    t = threading.Thread(target=run_pipeline,
                         args=(job_id, job_dir, codes, to_email, cc_email, send_flag),
                         daemon=True)
    t.start()

    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def status(job_id):
    if job_id not in JOBS:
        return jsonify({'error': 'Job no encontrado'}), 404
    job = JOBS[job_id]
    return jsonify({
        'status':      job.get('status'),
        'step':        job.get('step', 0),
        'logs':        job.get('logs', []),
        'error':       job.get('error'),
        'final_count': job.get('final_count'),
        'errors':      job.get('errors', []),
    })

@app.route('/download/<job_id>')
def download(job_id):
    if job_id not in JOBS:
        return jsonify({'error': 'Job no encontrado'}), 404
    zip_path = JOBS[job_id].get('zip_path')
    if not zip_path or not os.path.exists(zip_path):
        return jsonify({'error': 'ZIP no disponible'}), 404
    return send_file(zip_path, as_attachment=True,
                     download_name=os.path.basename(zip_path))

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
