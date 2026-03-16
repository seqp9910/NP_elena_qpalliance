# Elena NP — Notificaciones Personales
### Empowered by QPAlliance

Herramienta web para el procesamiento automatizado de Notificaciones Personales (NP) en procesos laborales.

---

## ¿Qué hace?

1. Recibe los **códigos** del lote (pegados directamente en pantalla)
2. Carga la **base de radicación** de demandas (Excel)
3. Recibe los **autos admisorios** (PDFs)
4. Recibe los **demandas** (PDFs nombradas como `R{código}DDD...`)
5. Genera un PDF por caso: `R{código}.DDD.NP.pdf` (Notificación + Separador + Demanda)
6. Envía cada PDF por correo vía Brevo
7. Genera los archivos `.done` (PDF + comprobante de envío)
8. Genera el **Dashboard** HTML del lote
9. Presenta todo en un ZIP para descargar

---

## Despliegue en Railway (recomendado)

1. **Forkea o sube este repositorio a GitHub**

2. **Crea una cuenta en [Railway](https://railway.app)** (gratis)

3. Haz clic en **"New Project" → "Deploy from GitHub repo"**

4. Selecciona este repositorio. Railway detecta el `Dockerfile` automáticamente.

5. En **Variables de entorno**, agrega:
   ```
   BREVO_API_KEY = tu_api_key_de_brevo
   ```

6. Railway hace el build y deploy automáticamente. En ~5 minutos tienes la URL.

---

## Despliegue en Render

1. Sube el repositorio a GitHub
2. En [Render](https://render.com) → New → Web Service → conecta el repo
3. Runtime: **Docker**
4. En Environment Variables agrega `BREVO_API_KEY`
5. Deploy

---

## Desarrollo local

```bash
# Instalar dependencias del sistema (Mac)
brew install libreoffice poppler

# Instalar dependencias Python
pip install -r requirements.txt

# Correr
python app.py
# → http://localhost:5000
```

---

## Variables de entorno

| Variable | Descripción | Requerida |
|---|---|---|
| `BREVO_API_KEY` | API key de Brevo para envío de correos | Sí |
| `PORT` | Puerto del servidor (default: 5000/8080) | No |

---

## Estructura de archivos esperada para las demandas

Los PDFs de demandas deben tener en su nombre `R{código}DDD`:
```
R1372DDD_rappi_demanda.pdf  ✅
R1496DDD.pdf               ✅
demanda_1372.pdf           ❌ (no detectado)
```

---

## Equipo QPAlliance Legal Department
notificacionesjudiciales@qpalliance.co
