"""
FanFaster Lexion — 5-bosqich: JAVOBNI OLIB KELISH
Hamma 5 bosqich BIR Gmail akkauntda ishlaydi (NotebookLM daftari bitta akkauntga bog'langan).
Ishga tushirish: python3.12 -m uvicorn main5:app --host 0.0.0.0 --port $PORT

Endpoint'lar:
    POST /api/fetch    {"notebook_id","delete":true} -> {"status":"done","answer"} | {"status":"pending"} | {"status":"error"}
    POST /api/delete   {"notebook_id"}               -> {"deleted"}
    GET  /api/diag/history?notebook_id=...           -> history xom chiqishi
Render env: FANFASTER_API_KEY, NLM_ACCOUNTS_JSON, NLM_MASTER_<PROFIL>_B64, PROTECTED_NOTEBOOK_IDS, MIN_GAP_SEC=1,
            NLM_HISTORY_ARGS (ixtiyoriy), CLEAN_MARKDOWN
"""
import asyncio
import base64
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, model_validator

# ───────────────────────── SOZLAMALAR ─────────────────────────
STAGE = 5
STAGE_NAME = "JAVOBNI OLIB KELISH"

API_KEY = os.environ.get("FANFASTER_API_KEY", "")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "https://fanfaster.uz").split(",") if o.strip()]

# Barcha 5 Renderda BIR XIL Gmail (profil). NLM_ACCOUNTS_JSON misol:
# [{"profile":"default","email":"x@gmail.com","umumiy":"UMUMIY-DAFTAR-ID","katalog":"KATALOG-DAFTAR-ID"}]
# Auth: NLM_MASTER_<PROFIL>_B64 (profil nomi KATTA harfda), avvalgi main.py bilan bir xil.
if os.environ.get("NLM_ACCOUNTS_JSON"):
    ACCOUNTS = json.loads(os.environ["NLM_ACCOUNTS_JSON"])
else:
    ACCOUNTS = [{"profile": "default", "email": os.environ.get("NLM_EMAIL", "")}]
ACC = ACCOUNTS[0]
P = ACC["profile"]

# Hech qachon o'chirilmaydigan / qo'shimcha manba qo'shilmaydigan daftarlar (katalog va umumiy daftar)
PROTECTED_IDS = {x.strip() for x in os.environ.get("PROTECTED_NOTEBOOK_IDS", "").split(",") if x.strip()}
PROTECTED_IDS |= {a[k] for a in ACCOUNTS for k in ("umumiy", "katalog") if a.get(k)}

MIN_GAP_SEC = float(os.environ.get("MIN_GAP_SEC", "3"))      # ketma-ket CLI chaqiruvlari orasidagi pauza
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))        # vaqtinchalik xatoda qayta urinishlar soni
MAX_SOURCES = 18
JOB_TTL_SEC = 3600                                           # tugagan vazifalar xotirada 1 soat turadi
AUTH_REFRESH_HOURS = float(os.environ.get("NLM_AUTH_REFRESH_HOURS", "6"))  # 0 = o'chirilgan
# Yengil CLI chaqiruvlari (create/source/history/delete) soni. Har chaqiruv ~90 MB RAM yeydi, shuning uchun cheklanadi.
# `ask` bunga kirmaydi (u HEAVY_ASK_SEM bilan alohida cheklanadi).
CLI_LIMIT = max(int(os.environ.get("CLI_LIMIT", "2")), 1)
CLI_SEM = threading.BoundedSemaphore(CLI_LIMIT)
# Hamma jarayon (create/source/history/delete) STANDART holda android backend orqali ishlaydi (RAM kam).
# NLM_LIGHT_BACKEND=web (yoki none) -> odatdagi backendga qaytarish. `ask` alohida: NLM_CUSTOM_ASK_BACKEND (3-bosqich).
_lb = os.environ.get("NLM_LIGHT_BACKEND", "android").strip().lower()
LIGHT_BACKEND = "" if _lb in ("", "web", "none") else _lb

NLM = shutil.which("notebooklm") or "notebooklm"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
QUOTA_WORDS = ("429", "quota", "limit exceeded", "rate limit", "resource exhausted")
TRANSIENT_WORDS = ("429", "rate", "quota", "limit", "timeout", "timed out", "temporar", "503", "500", "unavailable")
AUTH_WORDS = ("auth", "login", "expired", "401", "403", "unauthorized", "sign in", "cookie")
AUTH_USER_MSG = "AI xizmatiga ulanib bo'lmadi. Birozdan keyin qayta urinib ko'ring."


class NlmError(Exception):
    pass


class AuthError(NlmError):
    pass


class SourceError(NlmError):
    """Manba bilan bog'liq xato. `user_msg` ustozga ko'rsatilishi mumkin (brendsiz, neytral)."""

    def __init__(self, user_msg: str, detail: str = ""):
        super().__init__(detail or user_msg)
        self.user_msg = user_msg


class NoBasisError(SourceError):
    """Model "manba yo'q, tahlil qila olmayman" deb rad etdi. Ustozga tayyor neytral matn ko'rsatiladi."""


# ───────────────────────── HOLAT ─────────────────────────
TASKS = {}
TASKS_LOCK = threading.Lock()
GAP_LOCK = threading.Lock()
REFRESH_LOCK = threading.Lock()
LAST_CALL = [0.0]
STATS = {"email": ACC.get("email", ""), "ok": 0, "fail": 0, "auth_ok": True, "last_error": None, "last_refresh": None}
READY = {"session": False}


# ───────────────────────── CLI YORDAMCHILARI ─────────────────────────
def patch_chat_limit() -> None:
    """notebooklm-py'dagi DEFAULT_CHAT_RESPONSE_MAX_BYTES ni NLM_CHAT_MAX_MB ga o'zgartiradi (CLI har chaqiruvda yangidan import qiladi)."""
    chat_max_mb = max(int(os.environ.get("NLM_CHAT_MAX_MB", "0")), 0)
    if not chat_max_mb:
        return
    try:
        import importlib.util
        spec = importlib.util.find_spec("notebooklm")
        if not spec or not spec.submodule_search_locations:
            print("[patch] notebooklm paketi topilmadi, limit o'zgartirilmadi", flush=True)
            return
        path = os.path.join(list(spec.submodule_search_locations)[0], "_runtime", "config.py")
        src = open(path, encoding="utf-8").read()
        new_line = f"DEFAULT_CHAT_RESPONSE_MAX_BYTES = {chat_max_mb} * 1024 * 1024"
        pat = re.compile(r"^DEFAULT_CHAT_RESPONSE_MAX_BYTES\s*=.*$", re.M)
        if not pat.search(src):
            print("[patch] DEFAULT_CHAT_RESPONSE_MAX_BYTES topilmadi (kutubxona versiyasi boshqa)", flush=True)
            return
        if new_line in src:
            print(f"[patch] chat limiti allaqachon {chat_max_mb} MB", flush=True)
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write(pat.sub(new_line, src, count=1))
        print(f"[patch] chat javob limiti {chat_max_mb} MB ga o'zgartirildi", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[patch] limitni o'zgartirib bo'lmadi: {e}", flush=True)


def ensure_session(profile: str, force: bool = False) -> bool:
    """storage_state.json yo'q (yoki majburan) bo'lsa, master token'dan cookie'ni qayta yasaydi."""
    path = os.path.expanduser(f"~/.notebooklm/profiles/{profile}/storage_state.json")
    with REFRESH_LOCK:
        if os.path.exists(path) and not force:
            return True
        try:
            r = subprocess.run([NLM, "-p", profile, "auth", "refresh", "--quiet"],
                               capture_output=True, text=True, timeout=180)
        except subprocess.TimeoutExpired:
            print(f"[ensure_session] {profile}: timeout")
            return False
        if r.returncode != 0:
            print(f"[ensure_session] {profile}: {(r.stderr or r.stdout).strip()[:300]}")
            return False
        STATS["last_refresh"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return os.path.exists(path)


def nlm(profile: str, *args: str, timeout: int = 300, stdin: Optional[str] = None,
        backend: Optional[str] = None, heavy: bool = False, _retried: bool = False) -> str:
    """notebooklm CLI'ni chaqiradi. Chaqiruvlar orasida pauza qo'yadi.
    heavy=False (standart): bir vaqtda CLI_LIMIT tadan ko'p CLI jarayoni ochilmaydi (RAM himoyasi)."""
    with GAP_LOCK:
        wait = MIN_GAP_SEC - (time.time() - LAST_CALL[0])
        if wait > 0:
            time.sleep(wait)
        LAST_CALL[0] = time.time()
    sem = None if heavy else CLI_SEM
    if backend is None and not heavy and LIGHT_BACKEND:
        backend = LIGHT_BACKEND
    if sem:
        sem.acquire()
    try:
        cmd = [NLM, "-p", profile] + (["--backend", backend] if backend else []) + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=stdin)
    except subprocess.TimeoutExpired:
        raise NlmError("timeout: NotebookLM javob bermadi")
    finally:
        if sem:
            sem.release()
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "noma'lum xato").strip()[:500]
        low = msg.lower()
        if "not logged in" in low and not _retried and ensure_session(profile, force=True):
            return nlm(profile, *args, timeout=timeout, stdin=stdin, backend=backend, heavy=heavy, _retried=True)
        if any(w in low for w in AUTH_WORDS):
            raise AuthError(msg)
        raise NlmError(msg)
    return r.stdout.strip()


def with_retry(fn, retries: Optional[int] = None):
    """Vaqtinchalik (limit/timeout) xatolarda kutib qayta urinadi."""
    delay = 10
    n = MAX_RETRIES if retries is None else retries
    for attempt in range(n + 1):
        try:
            return fn()
        except AuthError:
            raise
        except NlmError as e:
            transient = any(w in str(e).lower() for w in TRANSIENT_WORDS)
            if not transient or attempt == n:
                raise
            time.sleep(delay)
            delay *= 3


def mem_mb() -> str:
    """Konteyner xotirasi (MB) -- Render shu qiymat bo'yicha serverni o'chiradi. O'qib bo'lmasa '?'."""
    for path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            with open(path) as f:
                return str(int(f.read().strip()) // (1024 * 1024))
        except (OSError, ValueError):
            continue
    return "?"


def step(tid: str, msg: str, t0: float) -> None:
    print(f"[step] s{STAGE} task={tid[:8]} +{time.time() - t0:.0f}s mem={mem_mb()}MB {msg}", flush=True)


# ───────────────────────── VAZIFALAR (asinxron bosqichlar uchun) ─────────────────────────
def new_task(kind: str, **extra) -> dict:
    tid = uuid.uuid4().hex
    t = {"id": tid, "kind": kind, "status": "queued", "created": time.time(), "result": None, "error": None,
         "code": None, **extra}
    with TASKS_LOCK:
        TASKS[tid] = t
    return t


def _error_code(e: Exception) -> str:
    if isinstance(e, AuthError):
        return "auth"
    if isinstance(e, NoBasisError):
        return "no_basis"
    if isinstance(e, SourceError):
        return "source"
    if isinstance(e, NlmError) and any(w in str(e).lower() for w in QUOTA_WORDS):
        return "quota"
    return "error"


def run_task(t: dict, fn) -> None:
    """fn(t) -> natija dict. Xatoda: status=error, code=auth|quota|source|no_basis|error, error=ustozga ko'rsatiladigan matn."""
    if t["status"] == "queued" and not t.get("manual_status"):
        t["status"] = "running"
    t["started"] = time.time()
    try:
        res = fn(t)
        t.update(status="done", result=res, finished=time.time())
        STATS["ok"] += 1
        STATS["auth_ok"] = True
    except Exception as e:  # noqa: BLE001
        STATS["fail"] += 1
        STATS["last_error"] = str(e)[:200]
        code = _error_code(e)
        if code == "auth":
            STATS["auth_ok"] = False
        print(f"[task {t['id'][:8]}] s{STAGE}: {e}", flush=True)   # xom xato faqat serverda
        user_msg = AUTH_USER_MSG if code == "auth" else (getattr(e, "user_msg", None) or "Javob tayyorlanmadi. Qayta urinib ko'ring.")
        t.update(status="error", error=user_msg, code=code, finished=time.time())


def start_task(t: dict, fn) -> None:
    threading.Thread(target=run_task, args=(t, fn), daemon=True).start()


def janitor():
    while True:
        time.sleep(300)
        now = time.time()
        with TASKS_LOCK:
            for tid in [k for k, v in TASKS.items() if v.get("finished") and now - v["finished"] > JOB_TTL_SEC]:
                TASKS.pop(tid, None)


def auth_refresher():
    # Birinchi yangilash ensure_session'da bo'lgan; 5 ta Render bir Gmail'ni bir vaqtda yangilamasligi uchun avval kutiladi
    while True:
        time.sleep(max(AUTH_REFRESH_HOURS, 0.1) * 3600)
        try:
            nlm(P, "auth", "refresh", "--quiet", timeout=120)
            STATS["last_refresh"] = time.strftime("%Y-%m-%d %H:%M:%S")
            STATS["auth_ok"] = True
        except NlmError as e:
            print(f"[auth refresh] {P}: {e}")
            STATS["last_error"] = str(e)[:200]


def restore_cookies():
    """Hosting uchun: env'dagi base64 fayllardan profil fayllarini tiklaydi (fayl yo'q bo'lsagina).
    NLM_MASTER_<PROFIL>_B64  -> master_token.json  (asosiy, o'zi cookie yangilaydi)
    NLM_STORAGE_<PROFIL>_B64 -> storage_state.json (ixtiyoriy)"""
    for acc in ACCOUNTS:
        pname = acc["profile"]
        for env_prefix, fname in (("NLM_MASTER_", "master_token.json"), ("NLM_STORAGE_", "storage_state.json")):
            b64 = os.environ.get(f"{env_prefix}{pname.upper()}_B64")
            path = os.path.expanduser(f"~/.notebooklm/profiles/{pname}/{fname}")
            if b64 and not os.path.exists(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(base64.b64decode(b64))
                os.chmod(path, 0o600)


class NotebookReq(BaseModel):
    notebook_id: str


def require_key(x_api_key: str = Header(default="")):
    if not API_KEY:
        raise HTTPException(503, "FANFASTER_API_KEY sozlanmagan")
    if not secrets.compare_digest(x_api_key.encode(), API_KEY.encode()):
        raise HTTPException(401, "Kalit noto'g'ri")


def check_notebook_id(nid: str, allow_protected: bool = False) -> str:
    nid = (nid or "").strip().lower()
    if not UUID_RE.fullmatch(nid):
        raise HTTPException(422, "notebook_id noto'g'ri")
    if nid in PROTECTED_IDS and not allow_protected:
        raise HTTPException(422, "Bu daftarga tegib bo'lmaydi")
    return nid


def http_fail(e: Exception) -> HTTPException:
    """Sinxron endpoint'larda CLI xatosini HTTP javobiga aylantiradi (xom matn ustozga chiqmaydi)."""
    code = _error_code(e)
    msg = AUTH_USER_MSG if code == "auth" else (getattr(e, "user_msg", None) or "Javob tayyorlanmadi. Qayta urinib ko'ring.")
    print(f"[s{STAGE}] {e}", flush=True)
    return HTTPException(503 if code in ("auth", "quota") else 502, {"code": code, "message": msg})


@asynccontextmanager
async def lifespan(app: FastAPI):
    patch_chat_limit()
    restore_cookies()
    READY["session"] = await asyncio.to_thread(ensure_session, P)
    threading.Thread(target=janitor, daemon=True).start()
    if AUTH_REFRESH_HOURS > 0:
        threading.Thread(target=auth_refresher, daemon=True).start()
    yield


app = FastAPI(title=f"FanFaster Lexion bosqich {STAGE}: {STAGE_NAME}", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)


@app.get("/api/task/{task_id}", dependencies=[Depends(require_key)])
def get_task(task_id: str):
    t = TASKS.get(task_id)
    if not t:
        raise HTTPException(404, "Vazifa topilmadi (muddati o'tgan yoki server qayta ishga tushgan)")
    return {
        "task_id": task_id, "stage": STAGE, "status": t["status"], "code": t.get("code"),
        "error": t.get("error"), "result": t.get("result"),
        "elapsed_sec": round((t.get("finished") or time.time()) - t["created"], 1),
    }


@app.get("/api/stats", dependencies=[Depends(require_key)])
def stats():
    master = os.path.exists(os.path.expanduser(f"~/.notebooklm/profiles/{P}/master_token.json"))
    running = sum(1 for t in TASKS.values() if t["status"] in ("queued", "running"))
    return {"stage": STAGE, "stage_name": STAGE_NAME, "ready": READY["session"], "mem_mb": mem_mb(),
            "active_tasks": running, "cli_limit": CLI_LIMIT, "master_token_present": master, "account": STATS}


@app.get("/")
@app.get("/ping")
def health_check():
    """FanFaster ishni boshlaganda hamma Renderga shuni yuboradi (uyg'otish). ready=true bo'lsa Render tayyor."""
    return {"status": "FanFaster Lexion bosqichi faol", "stage": STAGE, "stage_name": STAGE_NAME,
            "ready": READY["session"], "mem_mb": mem_mb()}


# ───────────────────────── JAVOBNI TOZALASH (eski main.py bilan bir xil) ─────────────────────────
OFFER_HINTS = ("xohlaysizmi", "istaysizmi", "internet", "qidirib", "batafsilroq", "yordam beraman", "yordam bera olaman")
BRAND_RE = re.compile(r"(?i)notebook\s?lm|gemini\s+notebook")
# Ustozga ko'rinmasligi kerak bo'lgan ichki so'zlar (daftar, baza, brend). Shunday so'z bor gap butunlay olib tashlanadi.
LEAK_RE = re.compile(r"(?i)notebook\s?lm|gemini\s+notebook|daftar|umumiy\s+baza")
# NotebookLM "manba yo'q, tahlil qila olmayman" degan holatni aniqlash iboralari
REFUSAL_HINTS = (
    "hech qanday manba", "manbalar qatorida bo'lmagani", "manbalar orasida yo'q",
    "imkoni yo'q", "amalga oshirish imkoni", "faqat daftar", "yuklangan manba",
    "manbalar mavjud emas", "manba yoki hujjat mavjud emas",
)
NO_BASIS_MSG_GENERAL = (
    "Bu kazus bo'yicha tegishli qonun matnlari topilmadi. "
    "Kazusga mos manba (qonun havolasi yoki matni) qo'shib, qayta urinib ko'ring."
)
NO_BASIS_MSG_SOURCES = (
    "Berilgan manbalarda bu kazusga tegishli norma topilmadi. "
    "Boshqa manba qo'shing yoki manbasiz qayta urinib ko'ring."
)
CLEAN_MARKDOWN = os.environ.get("CLEAN_MARKDOWN", "1") == "1"   # ###, **, --- belgilarini olib tashlash


def _norm(s: str) -> str:
    return re.sub(r"[‘’ʻʼ`´]", "'", s).lower()


def is_refusal(text: str) -> bool:
    """Qisqa javobda "manba yo'q / tahlil imkoni yo'q" iborasi bo'lsa va IRAC tuzilmasi bo'lmasa, rad deb hisoblanadi.
    Uzun yoki tuzilmali javobdagi "manbada yo'q" degan foydali gaplar rad hisoblanmaydi."""
    n = _norm(text)
    hit = any(h in n for h in REFUSAL_HINTS)
    structured = "issue" in n and "conclusion" in n
    return hit and len(text) < 1500 and not structured


def strip_markdown(txt: str) -> str:
    """###, **, --- belgilarini olib tashlaydi, ro'yxatni tirega aylantiradi, qattiq qator uzilishlarini birlashtiradi."""
    out_lines = []
    for ln in txt.splitlines():
        s = ln.rstrip()
        if re.match(r"^\s*([-*_])\1{2,}\s*$", s):          # --- yoki ***
            out_lines.append("")
            continue
        h = re.match(r"^\s*#{1,6}\s*(.*)$", s)
        if h:                                                # sarlavha: alohida abzats
            out_lines += ["", re.sub(r"\*\*|__", "", h.group(1)).strip(), ""]
            continue
        s = re.sub(r"^(\s*)[*•]\s+", r"\1- ", s)              # * bandi -> - bandi
        s = re.sub(r"\*\*|__", "", s)
        s = re.sub(r"(?<![*\w])\*(?!\s)([^*\n]+?)\*(?![*\w])", r"\1", s)   # *kursiv*
        out_lines.append(s)
    # qattiq qator uzilishlarini birlashtirish (ro'yxat bandi/raqamli band/sarlavha yangi qator boshlaydi)
    starts_new = re.compile(r"^\s*(\d+[.)]\s|[-•]\s)")
    head_kw = re.compile(r"(?i)^\s*(?:\d+[.)]\s*|[IVX]+[.)]\s*)?(issue|rule|application|conclusion)\b")
    merged = []
    for ln in out_lines:
        cur = ln.strip()
        if not cur:
            if merged and merged[-1] != "":
                merged.append("")
            continue
        prev_blank = (not merged) or merged[-1] == ""
        is_head = (head_kw.match(cur) and len(cur) <= 100) or \
                  (prev_blank and len(cur) <= 65 and not re.search(r"[.!?:;,]$", cur) and not starts_new.match(ln))
        if is_head:                                          # sarlavha: alohida abzats
            if merged and merged[-1] != "":
                merged.append("")
            merged += [cur, ""]
            continue
        if merged and merged[-1] != "" and not starts_new.match(ln):
            merged[-1] += " " + cur
        else:
            merged.append(ln.rstrip() if starts_new.match(ln) else cur)
    res = []
    for ln in merged:                                        # ko'p qatorga cho'zilgan *kursiv* ham endi bir qatorda
        ln = re.sub(r"(?<![*\w])\*(?!\s)([^*\n]+?)\*(?![*\w])", r"\1", ln)
        res.append(ln)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(res)).strip()


def strip_leaks(txt: str) -> str:
    """Ichki so'zlar (daftar, umumiy baza, NotebookLM) uchragan gaplarni olib tashlaydi."""
    res = []
    for ln in txt.splitlines():
        if not LEAK_RE.search(ln):
            res.append(ln)
            continue
        m = re.match(r"^(\s*(?:\d+[.)]|[-•])\s+)?(.*)$", ln)
        prefix, body = (m.group(1) or ""), m.group(2)
        kept = [x for x in re.split(r"(?<=[.!?])\s+", body) if x and not LEAK_RE.search(x)]
        if kept:
            res.append(prefix + " ".join(kept))
    return re.sub(r"\n{3,}", "\n\n", "\n".join(res)).strip()


def strip_trailing_offer(txt: str) -> str:
    """Javob oxiridagi "internetdan qidiraymi?" kabi savol/taklifni olib tashlaydi (oxirgi gap yoki 💡 abzats)."""
    txt = txt.strip()
    for _ in range(3):
        paras = re.split(r"\n\s*\n", txt)
        last = paras[-1].strip()
        if len(paras) > 1 and last.startswith(("💡", "🔍")):
            txt = "\n\n".join(paras[:-1]).strip()
            continue
        sents = re.split(r"(?<=[.!?])\s+", last)
        tail = sents[-1].strip()
        if tail.rstrip("*_ ").endswith("?") and any(h in tail.lower() for h in OFFER_HINTS):
            rest = " ".join(sents[:-1]).strip()
            txt = "\n\n".join(paras[:-1] + ([rest] if rest else [])).strip()
            continue
        break
    return txt


def clean_answer(raw: str, custom: bool = False) -> str:
    lines = [
        ln for ln in raw.splitlines()
        if not re.match(r"^\s*(Continuing conversation|Resumed conversation|Conversation:)", ln)
    ]
    txt = "\n".join(lines).strip()
    txt = re.sub(r"^Answer:\s*", "", txt).strip()
    if is_refusal(txt):
        raise NoBasisError(NO_BASIS_MSG_SOURCES if custom else NO_BASIS_MSG_GENERAL,
                           f"model rad etdi: {txt[:300]!r}")
    txt = re.sub(r"(?<=\w)[ʻʼ‘’`´](?=\w)", "'", txt)       # oʻ, gʻ va o', g' bir xil ko'rinsin
    if CLEAN_MARKDOWN:
        txt = strip_markdown(txt)
    txt = strip_trailing_offer(txt)
    txt = strip_leaks(txt)
    return BRAND_RE.sub("", txt)


# ───────────────────────── DAFTARNI O'CHIRISH ─────────────────────────
# ───────────────────────── KUTUBXONA ORQALI DAFTAR YARATISH/O'CHIRISH (jarayon ichida, CLI'siz) ─────────────────────────
# NLM_NB_LIB=0 -> eski CLI yo'li. Android'da `create` bo'lmasa, o'sha jarayon ichida web backend sinaladi, keyin CLI.
NB_LIB = os.environ.get("NLM_NB_LIB", "1").strip() != "0"


class LibUnavailable(Exception):
    """Kutubxona yo'li ishlamadi va hech narsa o'zgarmagan -> eski CLI yo'liga o'tish xavfsiz."""


def _nb_exc(e: Exception) -> Exception:
    return AuthError(str(e)[:500]) if any(w in str(e).lower() for w in AUTH_WORDS) else NlmError(str(e)[:500])


async def _lib_nb(op: str, arg: str, backend: Optional[str]):
    from notebooklm import NotebookLMClient
    kw = {"profile": P}
    if backend:
        kw["backend"] = backend
    async with await NotebookLMClient.from_storage(**kw) as client:
        if op == "create":
            nb = await client.notebooks.create(arg)
            return getattr(nb, "id", None)
        await client.notebooks.delete(arg)
        return True


def lib_nb(op: str, arg: str):
    backends = [LIGHT_BACKEND or None] + ([None] if LIGHT_BACKEND else [])
    last: Optional[Exception] = None
    for be in backends:
        try:
            return asyncio.run(_lib_nb(op, arg, be))
        except (ImportError, AttributeError, NotImplementedError, TypeError) as e:   # qo'llab-quvvatlanmaydi: keyingi backend
            last = e
            print(f"[nb-lib] {op} backend={be or 'web'} ishlamadi: {str(e)[:120]}", flush=True)
            continue
        except Exception as e:  # noqa: BLE001
            raise _nb_exc(e)
    raise LibUnavailable(str(last))


def delete_notebook(profile: str, nid: str) -> bool:
    """Faqat vaqtinchalik daftarni o'chiradi. Katalog va umumiy daftarlarga hech qachon tegmaydi."""
    if not nid or nid in PROTECTED_IDS:
        return False
    if NB_LIB:
        try:
            lib_nb("delete", nid)
            return True
        except LibUnavailable:
            pass
        except Exception as e:  # noqa: BLE001
            if "not found" in str(e).lower():       # allaqachon o'chirilgan
                return True
            print(f"[delete] kutubxona xato berdi, CLI'ga o'tildi: {str(e)[:120]}", flush=True)
    for args in (("delete", "--notebook", nid, "--yes"), ("delete", "--notebook", nid), ("delete", nid)):
        try:
            nlm(profile, *args, timeout=60, stdin="y\n")
            return True
        except NlmError as e:
            print(f"[delete] {profile} {nid}: {e}")
    return False


@app.post("/api/delete", dependencies=[Depends(require_key)])
def delete_endpoint(req: NotebookReq):
    """Tozalash: xato bo'lganda FanFaster shuni chaqiradi, vaqtinchalik daftar yetim qolmasin."""
    nid = check_notebook_id(req.notebook_id)
    return {"deleted": delete_notebook(P, nid)}


# ───────────────────────── NOTEBOOKLM SUHBAT TARIXI (4- va 5-bosqich) ─────────────────────────
# DIQQAT: bu blok `notebooklm history` buyrug'iga tayanadi. Uning chiqish formati bu fayl yozilganda
# tasdiqlanmagan edi, shuning uchun bir nechta variant sinaladi va /api/diag/history xom chiqishni ko'rsatadi.
# NLM_HISTORY_ARGS="--json" kabi qo'yilsa, faqat shu variant ishlatiladi.
HISTORY_ENV = os.environ.get("NLM_HISTORY_ARGS", "").split()
BAD_OPTION_WORDS = ("no such option", "unrecognized", "unexpected extra", "usage:", "invalid option", "got unexpected")
DONE_CACHE = set()          # javobi tayyor deb topilgan daftarlar (qayta CLI ochmaslik uchun)


def read_history(nid: str):
    """(xom_matn, ishlatilgan_variant). Qo'llab-quvvatlanmagan opsiyada keyingi variantga o'tadi."""
    tries = [HISTORY_ENV] if HISTORY_ENV else [["--json"], ["--show-all"], []]
    last: Optional[Exception] = None
    for extra in tries:
        try:
            out = with_retry(retries=1, fn=lambda: nlm(P, "history", "--notebook", nid, *extra, timeout=90))
            return out, (" ".join(extra) or "(oddiy)")
        except AuthError:
            raise
        except NlmError as e:
            last = e
            if any(w in str(e).lower() for w in BAD_OPTION_WORDS):
                continue
            raise
    raise last or NlmError("history o'qib bo'lmadi")


def _walk_answers(o, out: list) -> None:
    if isinstance(o, dict):
        for k, v in o.items():
            if str(k).lower() in ("answer", "response", "a") and isinstance(v, str):
                out.append(v)
            else:
                _walk_answers(v, out)
    elif isinstance(o, (list, tuple)):
        if len(o) == 2 and all(isinstance(x, str) for x in o):      # [savol, javob] juftligi
            out.append(o[1])
            return
        for x in o:
            _walk_answers(x, out)


def parse_history(text: str) -> dict:
    """-> {answer, turns, format, complete}. complete=True faqat JSON'dan o'qilgan (kesilmagan) javob uchun."""
    text = (text or "").strip()
    if not text:
        return {"answer": None, "turns": 0, "format": "empty", "complete": False}
    try:
        data = json.loads(text)
    except ValueError:
        data = None
    if data is not None:
        found: list = []
        _walk_answers(data, found)
        found = [a for a in found if a and a.strip()]
        return {"answer": found[-1] if found else None, "turns": len(found), "format": "json", "complete": True}
    rows = []
    for line in text.splitlines():            # Rich jadvali: │ ustun │ ustun │ (matn kesilgan bo'lishi mumkin)
        if "│" not in line:
            continue
        cells = [c.strip() for c in line.split("│") if c.strip()]
        if len(cells) >= 2 and cells[-1].lower() not in ("answer", "javob", "response"):
            rows.append(cells[-1])
    if rows:
        return {"answer": rows[-1], "turns": len(rows), "format": "table", "complete": False}
    return {"answer": None, "turns": 0, "format": "unknown", "complete": False}


@app.get("/api/diag/history", dependencies=[Depends(require_key)])
def diag_history(notebook_id: str):
    """Bir martalik tashxis: `notebooklm history` xom chiqishini ko'rsatadi (formatni aniqlash uchun)."""
    nid = check_notebook_id(notebook_id)
    try:
        text, variant = read_history(nid)
    except Exception as e:  # noqa: BLE001
        raise http_fail(e)
    info = parse_history(text)
    return {"variant": variant, "chars": len(text), "head": text[:1500],
            "parsed": {k: (v[:200] if isinstance(v, str) else v) for k, v in info.items()}}


# ───────────────────────── KUTUBXONA ORQALI TARIX/HOLAT (jarayon ichida, CLI'siz) ─────────────────────────
# NLM_HISTORY_LIB=0 -> eski CLI (`notebooklm history`) yo'liga qaytish. Kutubxona xato bersa ham avtomatik CLI'ga o'tadi.
HIST_LIB = os.environ.get("NLM_HISTORY_LIB", "1").strip() != "0"


def _lib_exc(e: Exception) -> Exception:
    return AuthError(str(e)[:500]) if any(w in str(e).lower() for w in AUTH_WORDS) else NlmError(str(e)[:500])


async def _lib_history(nid: str, want_status: bool) -> dict:
    from notebooklm import NotebookLMClient
    kw = {"profile": P}
    if LIGHT_BACKEND:
        kw["backend"] = LIGHT_BACKEND
    async with await NotebookLMClient.from_storage(**kw) as client:
        generating = None                      # None = holat bilmaymiz (backend qo'llab-quvvatlamasa)
        if want_status:
            try:
                generating = bool((await client.chat.session_status(nid)).generating)
            except (NotImplementedError, AttributeError, TypeError):
                generating = None
        pairs = await client.chat.get_history(nid, limit=5)     # oldest-first (savol, javob) juftliklari, matn TO'LIQ
        answer = (pairs[-1][1] or "").strip() if pairs else ""
        return {"generating": generating, "answer": answer or None, "turns": len(pairs)}


def lib_history(nid: str, want_status: bool = False) -> dict:
    try:
        return asyncio.run(_lib_history(nid, want_status))
    except Exception as e:  # noqa: BLE001
        raise _lib_exc(e)


# ───────────────────────── 5-BOSQICH: JAVOBNI OLIB KELISH + DAFTARNI O'CHIRISH ─────────────────────────
class FetchReq(BaseModel):
    notebook_id: str
    delete: bool = True      # tayyor javob olingach vaqtinchalik daftarni o'chiradi


@app.post("/api/fetch", dependencies=[Depends(require_key)])
def fetch_endpoint(req: FetchReq):
    """Tayyor javobni tarixdan oladi, eski main.py'dagi kabi tozalaydi va daftarni o'chiradi.
    status: done | pending (javob hali yo'q, daftar o'chirilmaydi) | error."""
    nid = check_notebook_id(req.notebook_id)
    info = None
    variant = "lib"
    if HIST_LIB:
        try:
            r = lib_history(nid)
            info = {"answer": r["answer"], "turns": r["turns"], "format": "lib", "complete": True}   # get_history matni kesilmaydi
        except AuthError as e:
            raise http_fail(e)
        except Exception as e:  # noqa: BLE001
            print(f"[s5] kutubxona yo'li xato berdi, CLI'ga o'tildi: {str(e)[:150]}", flush=True)
    if info is None:
        try:
            text, variant = read_history(nid)
        except Exception as e:  # noqa: BLE001
            raise http_fail(e)
        info = parse_history(text)
    if not info["answer"]:
        return {"status": "pending"}
    if not info["complete"]:
        # history javobni kesib ko'rsatdi (jadval). To'liq matn kafolatlanmagan -> o'chirmaymiz, FanFaster 3-bosqich vazifasidan oladi
        print(f"[s5] history to'liq emas (format={info['format']}, variant={variant})", flush=True)
        return {"status": "error", "code": "history_incomplete",
                "error": "Javobni to'liq o'qib bo'lmadi.", "deleted": False}
    out = {"status": "done"}
    try:
        out["answer"] = clean_answer(info["answer"], True)
    except SourceError as e:      # NoBasisError: model "manba yo'q" dedi — ustozga tayyor neytral matn
        out.update(status="error", code="no_basis", error=e.user_msg)
    if req.delete:
        out["deleted"] = delete_notebook(P, nid)
        DONE_CACHE.discard(nid)
    return out
