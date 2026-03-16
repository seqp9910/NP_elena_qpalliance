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

# Load logo base64 once at startup
_LOGO_B64 = ''
try:
    _LOGO_B64 = (ASSETS_DIR / 'logo_b64.txt').read_text().strip()
except Exception:
    pass

# ─── UTILS ───────────────────────────────────────────────────────────────────
def hoy_str():
    m = ['','enero','febrero','marzo','abril','mayo','junio',
         'julio','agosto','septiembre','octubre','noviembre','diciembre']
    t = datetime.date.today()
    return f"{t.day:02d} de {m[t.month]} de {t.year}"

MESES_STR = ['', 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
             'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
MESES_MAP  = {m: i for i, m in enumerate(MESES_STR) if m}

def fecha_a_letras(fecha_str: str) -> str:
    """Convert date string to Spanish letters: '05 de enero de 2025'."""
    if not fecha_str:
        return fecha_str
    s = str(fecha_str).strip()
    # Already in letter format?
    if re.search(r'\bde\s+(' + '|'.join(MESES_STR[1:]) + r')\b', s, re.I):
        return s
    # dd/mm/yyyy or dd-mm-yyyy
    m = re.match(r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$', s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= month <= 12:
            return f"{day:02d} de {MESES_STR[month]} de {year}"
    return s

def parse_fecha(s) -> datetime.date | None:
    """Parse a date from string or date/datetime object. Returns datetime.date or None."""
    if isinstance(s, datetime.datetime):
        return s.date()
    if isinstance(s, datetime.date):
        return s
    s = str(s).strip()
    # dd de mes de yyyy
    m = re.match(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', s, re.I)
    if m:
        d, mes, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month = MESES_MAP.get(mes)
        if month:
            try:
                return datetime.date(y, month, d)
            except Exception:
                pass
    # dd/mm/yyyy or dd-mm-yyyy
    m = re.match(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', s)
    if m:
        try:
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except Exception:
            pass
    return None

def extract_fecha_admite_from_pdf(pdf_path: Path) -> str | None:
    """
    Try to extract the admission date from an auto admisorio PDF.
    Returns date as '05 de enero de 2025' or None if not found.
    Tries pdfplumber text extraction first (handles digital PDFs).
    """
    MESES_RE = '(' + '|'.join(MESES_STR[1:]) + ')'
    try:
        import pdfplumber
        text = ''
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages[:5]:
                t = page.extract_text()
                if t:
                    text += t + '\n'
    except Exception:
        return None

    if not text.strip():
        return None

    # Pattern 1: dd de mes de yyyy
    m = re.search(r'\b(\d{1,2})\s+de\s+' + MESES_RE + r'\s+de\s+(\d{4})\b',
                  text, re.IGNORECASE)
    if m:
        day, month_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        return f"{day:02d} de {month_str} de {year}"

    # Pattern 2: dd/mm/yyyy or dd-mm-yyyy
    m = re.search(r'\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b', text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{day:02d} de {MESES_STR[month]} de {year}"

    return None

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
    Returns dict: {code_int: {'Nombre', 'Radicado', 'Ciudad', 'Juzgado',
                               'Fecha_Demanda', 'email'}}
    """
    import pandas as pd
    import openpyxl

    try:
        wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
        sheets = wb.sheetnames
        wb.close()
    except Exception:
        sheets = []

    if 'Total' in sheets:
        sheet_name = 'Total'
    elif sheets:
        sheet_name = sheets[0]
    else:
        sheet_name = 0

    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=0)

    col_map = {}
    for col in df.columns:
        norm = normalizar(str(col))
        if norm == '#':
            col_map['num'] = col
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
        elif 'fecha' in norm and 'demanda' in norm:
            col_map.setdefault('fecha_demanda', col)
        elif 'correo' in norm or 'email' in norm or 'electroni' in norm or 'direcc' in norm:
            col_map.setdefault('email', col)

    result = {}
    num_col = col_map.get('num')
    if num_col is None:
        if '#' in df.columns:
            num_col = '#'
        else:
            raise ValueError(
                f"No se encontro columna de codigo. "
                f"Columnas: {list(df.columns)}"
            )

    for _, row in df.iterrows():
        try:
            raw_code = row[num_col]
            if pd.isna(raw_code):
                continue
            code_str = str(raw_code).strip()
            m = re.match(r'^[Rr]?(\d+)', code_str)
            if not m:
                continue
            code = int(m.group(1))
        except Exception:
            continue

        def get_col(key, default=''):
            col = col_map.get(key)
            if not col:
                return default
            val = row.get(col, default)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return str(val).strip()

        # Fecha_Demanda: convert to legible string
        fd_col = col_map.get('fecha_demanda')
        fd_raw = row.get(fd_col) if fd_col else None
        if fd_raw is not None and not (isinstance(fd_raw, float) and pd.isna(fd_raw)):
            try:
                if isinstance(fd_raw, (datetime.datetime, datetime.date)):
                    fecha_demanda = fecha_a_letras(fd_raw.strftime('%d/%m/%Y'))
                else:
                    fecha_demanda = fecha_a_letras(str(fd_raw).strip())
            except Exception:
                fecha_demanda = str(fd_raw).strip()
        else:
            fecha_demanda = ''

        result[code] = {
            'Nombre':        get_col('nombre'),
            'Radicado':      get_col('radicado'),
            'Ciudad':        get_col('ciudad'),
            'Juzgado':       get_col('juzgado'),
            'Fecha_Demanda': fecha_demanda,
            'email':         get_col('email'),
        }

    return result

def _fix_split_placeholders(xml_text: str, replacements: dict) -> str:
    """
    Replace {FieldName} placeholders in DOCX XML even when split across 3 w:r runs:
      run1: <w:t>{ or ' {'</w:t>
      run2: <w:t>FieldName</w:t>
      run3: <w:t>}</w:t>
    Preserves any leading space before the { in the first run.
    """
    for field, value in replacements.items():
        if str(field).startswith('__'):
            continue  # Skip internal flags
        safe_val = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

        # Pass 1: simple single-run replacements
        for variant in ['{'+field+'}', '{ '+field+'}', '{'+field+' }', '{ '+field+' }']:
            xml_text = xml_text.replace(variant, safe_val)

        # Pass 2: split-run — field name appears alone in a w:t element
        field_pat = r'<w:t(?:\s[^>]*)?>' + re.escape(field) + r'</w:t>'
        for m_field in list(re.finditer(field_pat, xml_text)):
            pos = m_field.start()

            before = xml_text[max(0, pos-1500):pos]
            opens = list(re.finditer(r'<w:t(?:\s[^>]*)?>[ ]?\{</w:t>', before))
            if not opens:
                continue
            last_open = opens[-1]
            open_abs_start = max(0, pos-1500) + last_open.start()
            open_abs_end   = max(0, pos-1500) + last_open.end()

            after = xml_text[m_field.end():m_field.end()+500]
            m_close = re.search(r'<w:t(?:\s[^>]*)?>}</w:t>', after)
            if not m_close:
                continue
            close_abs_start = m_field.end() + m_close.start()
            close_abs_end   = m_field.end() + m_close.end()

            # Preserve leading space before { when replacing
            new_open = re.sub(
                r'(<w:t[^>]*>)([ ]?)\{(</w:t>)',
                lambda m, val=safe_val: m.group(1) + m.group(2) + val + m.group(3),
                xml_text[open_abs_start:open_abs_end]
            )
            new_field = re.sub(
                r'(<w:t[^>]*>)' + re.escape(field) + r'(</w:t>)',
                lambda m: m.group(1) + m.group(2),
                xml_text[m_field.start():m_field.end()]
            )
            new_close = re.sub(
                r'(<w:t[^>]*>)}(</w:t>)',
                lambda m: m.group(1) + m.group(2),
                xml_text[close_abs_start:close_abs_end]
            )

            xml_text = (xml_text[:open_abs_start] + new_open +
                        xml_text[open_abs_end:m_field.start()] + new_field +
                        xml_text[m_field.end():close_abs_start] + new_close +
                        xml_text[close_abs_end:])
            break
    return xml_text


def fill_template(template_path: Path, data: dict, output_path: Path,
                  no_date_mode: bool = False):
    """
    Replace {placeholder} in DOCX template (handles split-run XML) and save.
    If no_date_mode=True, replaces the 'adiado {fecha_admite}' segment with
    '-y que aquí se adjunta-' (alternate paragraph when date not found).
    """
    import shutil as _shutil
    import zipfile as _zipfile

    _shutil.copy2(template_path, output_path)

    with _zipfile.ZipFile(output_path, 'r') as z:
        names = z.namelist()
        contents = {n: z.read(n) for n in names}

    all_data = dict(data)
    all_data['fecha_de_hoy'] = hoy_str()
    if no_date_mode:
        all_data['fecha_admite'] = '__NOFECHA__'

    def replace_in_xml(xml_bytes: bytes) -> bytes:
        text = xml_bytes.decode('utf-8')
        text = _fix_split_placeholders(text, all_data)
        if no_date_mode:
            # Remove ', adiado ' from the run that precedes {fecha_admite}
            text = text.replace('demanda, adiado </w:t>', 'demanda</w:t>')
            text = text.replace('demanda, adiado</w:t>', 'demanda</w:t>')
            # Replace the dummy token with alternate clause
            text = text.replace('__NOFECHA__', '-y que aqu\u00ed se adjunta-')
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

def build_separator_page(output_path: Path, text: str = 'DEMANDA'):
    """Build a single-page PDF with text centered at mid-page (Caladea Bold 72pt)."""
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    CALADEA = '/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf'
    font_name = 'CalaDeaBold'
    try:
        pdfmetrics.registerFont(TTFont(font_name, CALADEA))
    except Exception:
        font_name = 'Helvetica-Bold'

    w, h = letter
    c = canvas.Canvas(str(output_path), pagesize=letter)
    c.setFont(font_name, 72)
    text_w = c.stringWidth(text, font_name, 72)
    c.drawString((w - text_w) / 2, h / 2, text)
    c.save()


def build_email_proof_pdf(output_path: Path, code: int, client: dict,
                          to_email: str, sent_ok: bool, sent_msg: str,
                          email_subject: str, email_body_text: str):
    """Build a PDF page showing the sent email details as proof of dispatch."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title2', parent=styles['Heading1'],
                                 fontSize=14, textColor=colors.HexColor('#D4006A'))
    label_style = ParagraphStyle('Label', parent=styles['Normal'],
                                 fontSize=9, textColor=colors.grey)
    value_style = ParagraphStyle('Value', parent=styles['Normal'], fontSize=10)
    body_style  = ParagraphStyle('Body', parent=styles['Normal'],
                                 fontSize=9, leading=13)

    doc = SimpleDocTemplate(str(output_path), pagesize=letter,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    elements = []

    status_color = '#006600' if sent_ok else '#CC0000'
    status_text  = 'ENVIADO ✓' if sent_ok else 'ERROR AL ENVIAR'

    elements.append(Paragraph('Constancia de Envío — Notificación Personal', title_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width='100%', thickness=1,
                                color=colors.HexColor('#D4006A')))
    elements.append(Spacer(1, 10))

    fields = [
        ('Estado',       f'<font color="{status_color}"><b>{status_text}</b></font>'),
        ('Fecha y hora', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')),
        ('De',           SENDER_EMAIL),
        ('Para',         to_email),
        ('Asunto',       email_subject),
        ('Código',       f'R{code}'),
        ('Nombre',       client.get('Nombre', '')),
        ('Radicado',     client.get('Radicado', '')),
    ]
    if not sent_ok:
        fields.append(('Detalle error', sent_msg[:200]))

    for label, value in fields:
        elements.append(Paragraph(label, label_style))
        elements.append(Paragraph(str(value), value_style))
        elements.append(Spacer(1, 4))

    elements.append(Spacer(1, 10))
    elements.append(HRFlowable(width='100%', thickness=0.5, color=colors.grey))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph('Cuerpo del correo:', label_style))
    elements.append(Spacer(1, 4))
    plain_body = re.sub(r'<[^>]+>', '', email_body_text).strip()
    for line in plain_body.split('\n'):
        line = line.strip()
        if line:
            elements.append(Paragraph(line, body_style))

    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        f'Generado por Elena NP — QPAlliance — {hoy_str()}',
        ParagraphStyle('Footer', parent=styles['Normal'],
                       fontSize=8, textColor=colors.grey)))
    doc.build(elements)


def build_email_signature() -> str:
    """Build HTML email signature with QPAlliance logo."""
    logo_img = (
        f'<img src="data:image/png;base64,{_LOGO_B64}" '
        f'style="width:90px;height:auto;display:block;" alt="QPAlliance">'
    ) if _LOGO_B64 else '<strong style="color:#D4006A;font-size:16px">qpa</strong>'

    return f"""<br><br>
<hr style="border:none;border-top:2px solid #D4006A;margin:20px 0;max-width:420px">
<table cellpadding="0" cellspacing="0" style="font-family:Arial,sans-serif;font-size:13px;color:#333">
  <tr>
    <td style="padding-right:14px;vertical-align:middle">{logo_img}</td>
    <td style="vertical-align:middle;padding-left:14px;border-left:3px solid #D4006A;line-height:1.6">
      <strong style="font-size:14px;color:#222">Legal Department | QPAlliance</strong><br>
      <span style="color:#666">www.qpalliance.co</span><br>
      <span style="color:#666">notificacionesjudiciales@qpalliance.co</span>
    </td>
  </tr>
</table>"""


def send_email_brevo(to_email: str, to_name: str, subject: str,
                     html_body: str, attachment_paths: list = None):
    """Send email via Brevo transactional API with multiple attachments and signature."""
    import urllib.request
    if not BREVO_API_KEY:
        return False, "BREVO_API_KEY no configurada"

    full_body = html_body + build_email_signature()

    payload = {
        "sender":  {"name": SENDER_NAME, "email": SENDER_EMAIL},
        "to":      [{"email": to_email, "name": to_name}],
        "subject": subject,
        "htmlContent": full_body,
    }

    attachments = []
    for att_path in (attachment_paths or []):
        if att_path is None:
            continue
        p = Path(att_path)
        if p.exists() and p.stat().st_size > 0:
            with open(p, 'rb') as f:
                content_b64 = base64.b64encode(f.read()).decode()
            attachments.append({"content": content_b64, "name": p.name})
    if attachments:
        payload["attachment"] = attachments

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


def build_output_excel(cases_info: list, output_path: Path):
    """Build Excel output with processing metrics per case."""
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Lote NP"

    headers = [
        'Código', 'Nombre', 'Radicado', 'Ciudad',
        'Fecha Radicación Demanda', 'Fecha Admite', 'Resta (días)',
        'Procesó AA', 'Enviado'
    ]

    pink = PatternFill("solid", fgColor="D4006A")
    white_bold = Font(color="FFFFFF", bold=True)
    center = Alignment(horizontal='center')

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.fill = pink
        cell.font = white_bold
        cell.alignment = center

    for i, case in enumerate(cases_info, 2):
        fecha_dem = case.get('Fecha_Demanda', '')
        fecha_adm = case.get('fecha_admite_extracted', '')

        resta = ''
        if fecha_dem and fecha_adm:
            try:
                d1 = parse_fecha(fecha_dem)
                d2 = parse_fecha(fecha_adm)
                if d1 and d2:
                    resta = abs((d2 - d1).days)
            except Exception:
                resta = ''

        row_data = [
            f"R{case.get('code', '')}",
            case.get('Nombre', ''),
            case.get('Radicado', ''),
            case.get('Ciudad', ''),
            str(fecha_dem),
            str(fecha_adm) if fecha_adm else '',
            resta,
            '✓' if case.get('procesó_aa') else '✗',
            '✓' if case.get('enviado') else '✗',
        ]

        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=i, column=col, value=value)
            cell.alignment = Alignment(horizontal='center' if col in (1, 7, 8, 9) else 'left')
            if col == 8:
                cell.font = Font(color='006600' if case.get('procesó_aa') else 'CC0000', bold=True)
            elif col == 9:
                cell.font = Font(color='006600' if case.get('enviado') else 'CC0000', bold=True)

    for col in ws.columns:
        max_len = max((len(str(cell.value or '')) for cell in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 45)

    wb.save(str(output_path))


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
        log(f"OK: {len(excel_data)} registros cargados. Primeros codigos: {sample_keys}")
        log(f"Codigos solicitados: {codigos}")

        found    = [c for c in codigos if c in excel_data]
        missing  = [c for c in codigos if c not in excel_data]
        log(f"Encontrados: {found} | No encontrados: {missing}")
        if not found:
            log("ERROR: Ninguno de los codigos existe en el Excel.")
            job['status'] = 'error'
            job['error']  = f"Codigos {codigos} no encontrados. Primeros en Excel: {sample_keys}"
            return

        # STEP 2 — Validate documents exist for each code
        log("Validando documentos requeridos...", step=2)
        valid_codes = []
        for code in found:
            aa_pdf = find_pdf_for_code(code, autos_pdfs)
            dm_pdf = find_pdf_for_code(code, demandas_pdfs)
            missing_docs = []
            if not aa_pdf:
                missing_docs.append('auto admisorio')
            if not dm_pdf:
                missing_docs.append('demanda')
            if missing_docs:
                log(f"  ERROR R{code}: Faltan documentos requeridos: {', '.join(missing_docs)}. Omitiendo.")
            else:
                valid_codes.append(code)

        if not valid_codes:
            job['status'] = 'error'
            job['error']  = "Ningún código tiene todos los documentos requeridos (auto admisorio + demanda)."
            log("ERROR: No hay codigos validos con todos los documentos.")
            return

        # STEP 3 — Generate NP, send email, build proof, assemble final PDF per client
        log("Generando notificaciones personales...", step=3)
        cases_info = []
        paquetes   = []

        for code in valid_codes:
            row          = excel_data[code]
            nombre       = row.get('Nombre', '')
            radicado     = row.get('Radicado', '')
            ciudad       = row.get('Ciudad', '')
            juzgado      = row.get('Juzgado', '')
            fecha_demanda = row.get('Fecha_Demanda', '')

            # Locate uploaded PDFs (guaranteed to exist after validation)
            auto_pdf    = find_pdf_for_code(code, autos_pdfs)
            demanda_pdf = find_pdf_for_code(code, demandas_pdfs)

            # 3a. Extract fecha_admite from auto admisorio PDF
            fecha_admite_extracted = extract_fecha_admite_from_pdf(auto_pdf)
            no_date_mode = (fecha_admite_extracted is None)
            if fecha_admite_extracted:
                log(f"  Fecha admisión R{code}: {fecha_admite_extracted}")
            else:
                log(f"  No se pudo extraer fecha admisión de R{code} PDF — modo sin fecha")

            procesó_aa = (fecha_admite_extracted is not None)

            # 3b. Fill NP template → DOCX → PDF
            fill_data = {
                'Nombre':       nombre,
                'Radicado':     radicado,
                'Ciudad':       ciudad,
                'Juzgado':      juzgado,
                'fecha_admite': fecha_admite_extracted or '',
            }
            docx_out = job_dir / f"auto_{code}.docx"
            np_pdf   = None
            try:
                fill_template(template_path, fill_data, docx_out,
                              no_date_mode=no_date_mode)
                np_pdf = docx_to_pdf(docx_out, job_dir)
                log(f"  OK NP generada: R{code} - {nombre}")
            except Exception as e:
                log(f"  Error generando NP R{code}: {e}")

            # 3c. Build legal email body
            fecha_admite_display = fecha_admite_extracted or '(fecha pendiente)'
            email_subject = f"R{code} Notificación personal - {radicado} - {nombre}"
            email_body = (
                f"<p>Señores,<br>Rappi S.A.S.<br>Felipe Villamarín Lafaurie</p>"
                f"<p><strong>RADICADO:</strong> {radicado}<br>"
                f"<strong>REFERENCIA:</strong> Demanda ordinaria laboral promovida por "
                f"{nombre} en contra de Rappi SAS<br>"
                f"<strong>ASUNTO:</strong> Notificación personal de auto admisorio de "
                f"demanda ordinaria laboral de primera instancia.</p>"
                f"<p>Reciban un cordial saludo. De manera atenta, conforme lo dispuesto "
                f"por el artículo 8 de la Ley 2213 de 2022, nos permitimos notificarle "
                f"el auto del {fecha_admite_display}, por medio del cual se admite la demanda "
                f"que impetra nuestro cliente, {nombre}. A la presente se adjunta:</p>"
                f"<ol>"
                f"<li>Auto admisorio de la demanda.</li>"
                f"<li>Notificación personal.</li>"
                f"<li>Escrito de demanda.</li>"
                f"<li>Prueba 1.1.1 (video)</li>"
                f"<li>Pruebas documentales 1.1.2. a 1.1.16.</li>"
                f"<li>Poder debidamente otorgado</li>"
                f"<li>Anexos: Certificado de existencia y representación legal de la firma "
                f"de abogados QPALLIANCE SAS, certificado de existencia y representación "
                f"legal de Rappi SAS</li>"
                f"<li>Proyecto de liquidación de pretensiones.</li>"
                f"</ol>"
            )

            # 3d. Build DEMANDA separator page
            sep_pdf = job_dir / f"sep_{code}.pdf"
            try:
                build_separator_page(sep_pdf, 'DEMANDA')
            except Exception as e:
                log(f"  Error separador R{code}: {e}")
                sep_pdf = None

            # 3e. Build email demanda attachment: separator + demanda merged
            email_demanda_pdf = None
            if sep_pdf and sep_pdf.exists() and demanda_pdf:
                email_demanda_pdf = job_dir / f"demanda_att_{code}.pdf"
                try:
                    merge_pdfs([sep_pdf, demanda_pdf], email_demanda_pdf)
                except Exception as e:
                    log(f"  Error merging demanda attachment R{code}: {e}")
                    email_demanda_pdf = None

            # 3f. Send individual email with 3 attachments:
            #     1) NP PDF, 2) Auto admisorio PDF, 3) Separator+Demanda PDF
            sent_ok  = False
            sent_msg = 'BREVO_API_KEY no configurada'
            if dest_email and BREVO_API_KEY:
                log(f"  Enviando correo R{code} → {dest_email}...")
                attachments = [
                    np_pdf if (np_pdf and np_pdf.exists()) else None,
                    auto_pdf,
                    email_demanda_pdf,
                ]
                sent_ok, sent_msg = send_email_brevo(
                    dest_email, dest_email, email_subject, email_body, attachments)
                log(f"  {'OK correo enviado' if sent_ok else 'Error correo'} R{code}: {sent_msg[:80]}")
            else:
                log(f"  Email no enviado R{code} (BREVO_API_KEY no configurada)")

            # 3g. Build email proof page
            proof_pdf = job_dir / f"proof_{code}.pdf"
            try:
                build_email_proof_pdf(proof_pdf, code, row,
                                      dest_email or '(sin destinatario)',
                                      sent_ok, sent_msg, email_subject, email_body)
            except Exception as e:
                log(f"  Error constancia correo R{code}: {e}")
                proof_pdf = None

            # 3h. Merge final output PDF: NP → email proof → DEMANDA separator → demanda
            parts = []
            if np_pdf and np_pdf.exists():
                parts.append(np_pdf)
            if proof_pdf and proof_pdf.exists():
                parts.append(proof_pdf)
            if sep_pdf and sep_pdf.exists():
                parts.append(sep_pdf)
            if demanda_pdf:
                parts.append(demanda_pdf)

            if parts:
                paquete_path = job_dir / f"R{code}.DDD.NP.done.pdf"
                try:
                    merge_pdfs(parts, paquete_path)
                    paquetes.append(paquete_path)
                    log(f"  OK R{code}.DDD.NP.done.pdf ({len(parts)} partes)")
                except Exception as e:
                    log(f"  Error ensamblando R{code}: {e}")
            else:
                log(f"  Sin partes para R{code}, omitiendo.")

            cases_info.append({
                'code':               code,
                'Nombre':             nombre,
                'Radicado':           radicado,
                'Ciudad':             ciudad,
                'Juzgado':            juzgado,
                'Fecha_Demanda':      fecha_demanda,
                'fecha_admite_extracted': fecha_admite_extracted or '',
                'procesó_aa':         procesó_aa,
                'enviado':            sent_ok,
            })

        # STEP 4 — Build output Excel + constancia
        log("Generando Excel de métricas...", step=4)
        excel_out = job_dir / f'Reporte_NP_{job_id[:8]}.xlsx'
        try:
            build_output_excel(cases_info, excel_out)
            log(f"  OK Excel generado: {excel_out.name}")
        except Exception as e:
            log(f"  Error generando Excel: {e}")
            excel_out = None

        receipt_path = job_dir / 'constancia_NP.pdf'
        try:
            build_receipt_pdf(cases_info, receipt_path)
            log("  OK constancia generada.")
        except Exception as e:
            log(f"  Error generando constancia: {e}")
            receipt_path = None

        # STEP 5 — ZIP
        log("Empaquetando archivos finales...", step=5)
        zip_path = job_dir / f'NP_lote_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for p in paquetes:
                zf.write(p, p.name)
            if receipt_path and receipt_path.exists():
                zf.write(receipt_path, receipt_path.name)
            if excel_out and excel_out.exists():
                zf.write(excel_out, excel_out.name)
        log(f"  OK ZIP: {zip_path.name} ({zip_path.stat().st_size // 1024} KB)")

        job['status']    = 'done'
        job['zip_path']  = str(zip_path)
        job['paquetes']  = len(paquetes)
        job['total']     = len(codigos)
        job['cases']     = cases_info
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
    raw_codigos = request.form.get('codigos', '').strip()
    if not raw_codigos:
        return jsonify({'error': 'No se ingresaron codigos.'}), 400

    codigos = parse_codigos(raw_codigos)
    if not codigos:
        return jsonify({
            'error': 'No se ingresaron codigos validos. '
                     'Ingrese numeros separados por ";" (ej: 1372;1496)'
        }), 400

    job_id  = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True)

    excel_file = request.files.get('excel') or request.files.get('base_file')
    if not excel_file or not excel_file.filename:
        return jsonify({'error': 'Debe subir el archivo Excel de base.'}), 400

    excel_path = job_dir / 'base.xlsx'
    excel_file.save(str(excel_path))

    autos_files = request.files.getlist('autos_pdfs') or request.files.getlist('autos')
    autos_pdfs  = []
    autos_dir   = job_dir / 'autos'
    autos_dir.mkdir()
    for f in autos_files:
        if f and f.filename:
            p = autos_dir / f.filename
            f.save(str(p))
            autos_pdfs.append(p)

    demandas_files = request.files.getlist('demandas_pdfs') or request.files.getlist('demandas')
    demandas_pdfs  = []
    demandas_dir   = job_dir / 'demandas'
    demandas_dir.mkdir()
    for f in demandas_files:
        if f and f.filename:
            p = demandas_dir / f.filename
            f.save(str(p))
            demandas_pdfs.append(p)

    dest_email = (request.form.get('email') or request.form.get('email_to') or '').strip()

    JOBS[job_id] = {
        'status':  'queued',
        'step':    0,
        'log':     [],
        'codigos': codigos,
    }

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
    total    = job.get('total', len(job.get('codigos', [])))
    step     = job.get('step', 0)
    paquetes = job.get('paquetes', 0)
    progress = int((step / 6) * 100) if step else 0
    return jsonify({
        'status':    job['status'],
        'step':      step,
        'progress':  progress,
        'messages':  job.get('log', []),
        'log':       job.get('log', []),
        'paquetes':  paquetes,
        'sent':      paquetes,
        'total':     total,
        'error':     job.get('error', ''),
        'dash_path': job['status'] == 'done',
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
        num_col = '#' if '#' in df.columns else cols[0]
        sample = [str(row[num_col]) for _, row in df.head(10).iterrows()]
        return jsonify({
            'sheets': sheets, 'sheet_used': sheet_name,
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
    errores  = total - paquetes

    # Build chart data
    ciudad_counts = {}
    tramite_labels = []
    tramite_data   = []

    for c in cases:
        ciudad = c.get('Ciudad', 'N/A') or 'N/A'
        ciudad_counts[ciudad] = ciudad_counts.get(ciudad, 0) + 1

        fd = c.get('Fecha_Demanda', '')
        fa = c.get('fecha_admite_extracted', '')
        if fd and fa:
            d1 = parse_fecha(fd)
            d2 = parse_fecha(fa)
            if d1 and d2:
                tramite_labels.append(f"R{c.get('code','')}")
                tramite_data.append(abs((d2 - d1).days))

    ciudad_labels = json.dumps(list(ciudad_counts.keys()))
    ciudad_values = json.dumps(list(ciudad_counts.values()))
    tram_labels   = json.dumps(tramite_labels)
    tram_values   = json.dumps(tramite_data)
    prom_dias     = round(sum(tramite_data) / len(tramite_data), 1) if tramite_data else 'N/A'

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Dashboard NP</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#0f0f1a;color:#e0e0e0;padding:28px}}
  h2{{color:#e91e8c;margin-bottom:4px;font-size:1.3rem}}
  .sub{{color:#888;font-size:.85rem;margin-bottom:24px}}
  .kpi-row{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:28px}}
  .kpi{{background:#1a1a2e;border-radius:12px;padding:18px 28px;min-width:140px;border:1px solid #2a2a3e}}
  .kpi .num{{font-size:2.4rem;font-weight:800;color:#e91e8c;line-height:1}}
  .kpi .lbl{{font-size:.8rem;color:#888;margin-top:6px;text-transform:uppercase;letter-spacing:.5px}}
  .kpi.ok .num{{color:#4caf50}}
  .kpi.err .num{{color:#f44336}}
  .kpi.prom .num{{color:#ff9800;font-size:1.6rem}}
  .charts{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:28px}}
  .chart-box{{background:#1a1a2e;border-radius:12px;padding:20px;border:1px solid #2a2a3e}}
  .chart-box h3{{color:#ccc;font-size:.95rem;margin-bottom:14px;font-weight:600}}
  .btn-row{{display:flex;gap:12px;margin-top:8px}}
  .btn{{padding:10px 24px;border-radius:8px;border:none;cursor:pointer;font-size:.9rem;font-weight:600}}
  .btn-primary{{background:#e91e8c;color:#fff}}
  .btn-primary:hover{{background:#c4006a}}
  .btn-outline{{background:transparent;color:#e91e8c;border:2px solid #e91e8c}}
  .btn-outline:hover{{background:#e91e8c;color:#fff}}
  @media(max-width:640px){{.charts{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<h2>Dashboard — Lote NP</h2>
<p class="sub">Generado el {hoy_str()}</p>

<div class="kpi-row">
  <div class="kpi">
    <div class="num">{total}</div>
    <div class="lbl">NP Procesadas</div>
  </div>
  <div class="kpi ok">
    <div class="num">{paquetes}</div>
    <div class="lbl">Exitosas</div>
  </div>
  <div class="kpi err">
    <div class="num">{errores}</div>
    <div class="lbl">Con error</div>
  </div>
  <div class="kpi prom">
    <div class="num">{prom_dias}</div>
    <div class="lbl">Días promedio trámite</div>
  </div>
</div>

<div class="charts">
  <div class="chart-box">
    <h3>Notificaciones Personales por Ciudad</h3>
    <canvas id="chart-ciudad" height="220"></canvas>
  </div>
  <div class="chart-box">
    <h3>Tiempo de Trámite por Caso (días)</h3>
    <canvas id="chart-tramite" height="220"></canvas>
  </div>
</div>

<div class="btn-row">
  <button class="btn btn-outline" onclick="window.open('/resumen/{job_id}','_blank')">📄 Resumen</button>
  <a href="/download/{job_id}" class="btn btn-primary">⬇️ Descargar ZIP</a>
</div>

<script>
const PINK = '#e91e8c';
const GREEN = '#4caf50';
const chartDefaults = {{
  plugins:{{legend:{{display:false}}}},
  scales:{{
    x:{{ticks:{{color:'#aaa',font:{{size:11}}}},grid:{{color:'#2a2a3e'}}}},
    y:{{ticks:{{color:'#aaa',font:{{size:11}}}},grid:{{color:'#2a2a3e'}}}}
  }}
}};

new Chart(document.getElementById('chart-ciudad'), {{
  type: 'bar',
  data: {{
    labels: {ciudad_labels},
    datasets: [{{
      data: {ciudad_values},
      backgroundColor: PINK + 'cc',
      borderColor: PINK,
      borderWidth: 1,
      borderRadius: 6,
    }}]
  }},
  options: {{...chartDefaults}}
}});

new Chart(document.getElementById('chart-tramite'), {{
  type: 'bar',
  data: {{
    labels: {tram_labels},
    datasets: [{{
      data: {tram_values},
      backgroundColor: GREEN + 'aa',
      borderColor: GREEN,
      borderWidth: 1,
      borderRadius: 6,
    }}]
  }},
  options: {{...chartDefaults, plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label: ctx => ctx.raw + ' días'}}}}}}}}
}});
</script>
</body>
</html>"""


@app.route('/resumen/<job_id>')
def resumen(job_id):
    """Printable summary with KPIs and charts (no individual table)."""
    job = JOBS.get(job_id)
    if not job:
        return "Job no encontrado", 404

    cases    = job.get('cases', [])
    paquetes = job.get('paquetes', 0)
    total    = job.get('total', 0)
    errores  = total - paquetes

    ciudad_counts = {}
    tramite_labels = []
    tramite_data   = []
    for c in cases:
        ciudad = c.get('Ciudad', 'N/A') or 'N/A'
        ciudad_counts[ciudad] = ciudad_counts.get(ciudad, 0) + 1
        fd = c.get('Fecha_Demanda', '')
        fa = c.get('fecha_admite_extracted', '')
        if fd and fa:
            d1 = parse_fecha(fd)
            d2 = parse_fecha(fa)
            if d1 and d2:
                tramite_labels.append(f"R{c.get('code','')}")
                tramite_data.append(abs((d2 - d1).days))

    ciudad_labels = json.dumps(list(ciudad_counts.keys()))
    ciudad_values = json.dumps(list(ciudad_counts.values()))
    tram_labels   = json.dumps(tramite_labels)
    tram_values   = json.dumps(tramite_data)
    prom_dias     = round(sum(tramite_data) / len(tramite_data), 1) if tramite_data else 'N/A'
    now_str       = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Resumen NP — QPAlliance</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#fff;color:#222;padding:32px;max-width:900px;margin:0 auto}}
  h1{{color:#D4006A;font-size:1.6rem;margin-bottom:4px}}
  .sub{{color:#888;font-size:.85rem;margin-bottom:24px}}
  .kpi-row{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:28px}}
  .kpi{{background:#fff5f9;border:1px solid #D4006A33;border-radius:10px;padding:16px 24px;min-width:120px}}
  .kpi .num{{font-size:2rem;font-weight:800;color:#D4006A}}
  .kpi .lbl{{font-size:.78rem;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px}}
  .charts{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:28px}}
  .chart-box{{border:1px solid #eee;border-radius:10px;padding:16px}}
  .chart-box h3{{color:#333;font-size:.9rem;margin-bottom:12px;font-weight:600}}
  .footer{{color:#aaa;font-size:.78rem;margin-top:32px;border-top:1px solid #eee;padding-top:12px}}
  .no-print{{margin-bottom:20px}}
  @media print{{.no-print{{display:none}}}}
</style>
</head>
<body>
<div class="no-print">
  <button onclick="window.print()" style="background:#D4006A;color:#fff;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;font-size:.9rem;font-weight:600">🖨️ Imprimir</button>
</div>

<h1>Resumen — Lote NP</h1>
<p class="sub">QPAlliance — Legal Department &nbsp;|&nbsp; Descargado el {now_str}</p>

<div class="kpi-row">
  <div class="kpi"><div class="num">{total}</div><div class="lbl">NP Procesadas</div></div>
  <div class="kpi"><div class="num">{paquetes}</div><div class="lbl">Exitosas</div></div>
  <div class="kpi"><div class="num">{errores}</div><div class="lbl">Con error</div></div>
  <div class="kpi"><div class="num">{prom_dias}</div><div class="lbl">Días promedio trámite</div></div>
</div>

<div class="charts">
  <div class="chart-box">
    <h3>NPs por Ciudad</h3>
    <canvas id="rc1" height="200"></canvas>
  </div>
  <div class="chart-box">
    <h3>Tiempo de Trámite por Caso (días)</h3>
    <canvas id="rc2" height="200"></canvas>
  </div>
</div>

<div class="footer">Generado por Elena NP — QPAlliance — {hoy_str()}</div>

<script>
new Chart(document.getElementById('rc1'),{{type:'bar',data:{{labels:{ciudad_labels},datasets:[{{data:{ciudad_values},backgroundColor:'#D4006Acc',borderColor:'#D4006A',borderWidth:1,borderRadius:4}}]}},options:{{plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{font:{{size:10}}}}}},y:{{ticks:{{font:{{size:10}}}}}}}}}}}} );
new Chart(document.getElementById('rc2'),{{type:'bar',data:{{labels:{tram_labels},datasets:[{{data:{tram_values},backgroundColor:'#4caf5099',borderColor:'#4caf50',borderWidth:1,borderRadius:4}}]}},options:{{plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:ctx=>ctx.raw+' días'}}}}}},scales:{{x:{{ticks:{{font:{{size:10}}}}}},y:{{ticks:{{font:{{size:10}}}}}}}}}}}} );
</script>
</body>
</html>"""

# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
