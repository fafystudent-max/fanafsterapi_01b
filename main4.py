"""
FanFaster Lexion — 4-bosqich: JAVOB TAYYORMI
Hamma 5 bosqich BIR Gmail akkauntda ishlaydi (NotebookLM daftari bitta akkauntga bog'langan).
Ishga tushirish: python3.12 -m uvicorn main4:app --host 0.0.0.0 --port $PORT

Endpoint'lar:
    POST /api/check    {"notebook_id"}           -> {"done": true|false}        (sinxron, faqat tekshiruv)
    GET  /api/diag/history?notebook_id=...       -> history xom chiqishi (bir martalik tashxis)
Render env: FANFASTER_API_KEY, NLM_ACCOUNTS_JSON, NLM_MASTER_<PROFIL>_B64, PROTECTED_NOTEBOOK_IDS, MIN_GAP_SEC=1,
            NLM_HISTORY_ARGS (ixtiyoriy), NLM_HISTORY_LIB=1 (standart; 0 -> eski CLI)
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
STAGE = 4
STAGE_NAME = "JAVOB TAYYORMI"

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


# ───────────────────────── 4-BOSQICH: JAVOB TAYYORMI? (faqat tekshiruv) ─────────────────────────
@app.post("/api/check", dependencies=[Depends(require_key)])
def check_endpoint(req: NotebookReq):
    """Javob NotebookLM tarixida paydo bo'lganmi? Matnni qaytarmaydi, faqat done: true/false.
    FanFaster har 15 soniyada chaqirsin."""
    nid = check_notebook_id(req.notebook_id)
    if nid in DONE_CACHE:
        return {"done": True, "cached": True}
    if HIST_LIB:
        try:
            r = lib_history(nid, want_status=True)
            # Google statusni oqim yopilishidan sal oldin "idle" qilishi mumkin, shuning uchun IKKALA shart: generating emas VA javob bor
            done = bool(r["answer"]) and r["generating"] is not True
            if done:
                if len(DONE_CACHE) > 500:
                    DONE_CACHE.clear()
                DONE_CACHE.add(nid)
            return {"done": done, "generating": r["generating"], "turns": r["turns"], "via": "lib"}
        except AuthError as e:
            raise http_fail(e)
        except Exception as e:  # noqa: BLE001
            print(f"[s4] kutubxona yo'li xato berdi, CLI'ga o'tildi: {str(e)[:150]}", flush=True)
    try:
        text, variant = read_history(nid)
    except Exception as e:  # noqa: BLE001
        raise http_fail(e)
    info = parse_history(text)
    done = bool(info["answer"])
    if done:
        if len(DONE_CACHE) > 500:
            DONE_CACHE.clear()
        DONE_CACHE.add(nid)
    return {"done": done, "turns": info["turns"], "format": info["format"], "variant": variant}
