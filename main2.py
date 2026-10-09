"""
FanFaster Lexion — 2-bosqich: MANBA QO'SHISH
Hamma 5 bosqich BIR Gmail akkauntda ishlaydi (NotebookLM daftari bitta akkauntga bog'langan).
Ishga tushirish: python3.12 -m uvicorn main2:app --host 0.0.0.0 --port $PORT

Endpoint'lar:
    POST /api/add      {"notebook_id","urls":[...]} -> {"task_id"}              (asinxron; GET /api/task/{id})
    result: {"notebook_id","sources"}; manbalar indekslanguncha vazifa "running" turadi.
Render env: FANFASTER_API_KEY, NLM_ACCOUNTS_JSON, NLM_MASTER_<PROFIL>_B64, PROTECTED_NOTEBOOK_IDS, NLM_BATCH_URLS=1,
            NLM_SOURCE_LIB=1 (standart; 0 -> eski CLI yo'li)
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
STAGE = 2
STAGE_NAME = "MANBA QO'SHISH"

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


# ───────────────────────── 2-BOSQICH: MANBA QO'SHISH ─────────────────────────
# NLM_BATCH_URLS=1 (standart) -> bir nechta havola bitta `source add-async` chaqiruvida qo'shiladi. Xato bo'lsa birma-bir qo'shishga qaytadi.
BATCH_URLS = os.environ.get("NLM_BATCH_URLS", "1").strip() != "0"
# NLM_MAX_URLS -> eng ko'pi bilan shuncha havola (0 = cheklovsiz)
MAX_URLS = max(int(os.environ.get("NLM_MAX_URLS", "0")), 0)
URL_ONLY_RE = re.compile(r"^https?://\S+$", re.I)
PENDING_STATUSES = {"processing", "pending", "preparing", "indexing", "uploading", "importing"}
BAD_STATUSES = {"error", "failed"}
SOURCE_FAIL_URL_MSG = "Manbalardan ba'zilarini ochib bo'lmadi. Havola o'rniga matnini «+ Matn» orqali qo'shing."
SOURCE_FAIL_TEXT_MSG = "Manbalardan ba'zilarini qo'shib bo'lmadi. Qayta urinib ko'ring."


class Source(BaseModel):
    """Manba: `url` (havola). Lexion bosqichida faqat havolalar keladi."""
    title: str = Field(default="Manba", max_length=200)
    content: Optional[str] = Field(default=None, max_length=300000)
    url: Optional[str] = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _check(self):
        if not (self.url or (self.content and self.content.strip())):
            raise ValueError("Manba uchun url yoki content kerak")
        if self.url and not self.url.strip().lower().startswith(("http://", "https://")):
            raise ValueError("url faqat http:// yoki https:// bo'lishi kerak")
        return self


def source_target(src: "Source"):
    """Manba turini aniqlaydi: ("url", havola) yoki ("text", matn)."""
    if src.url:
        return "url", src.url.strip()
    c = (src.content or "").strip()
    if URL_ONLY_RE.match(c):
        return "url", c
    return "text", c


def is_source_failure(msg: str) -> bool:
    low = msg.lower()
    return any(k in low for k in ("failed to add source", "add_source failed", "rpc_code=9", "could not be confirmed"))


def add_source(p: str, nid: str, src: "Source") -> None:
    kind, value = source_target(src)
    title = (src.title or "Manba")[:120]
    fail_msg = SOURCE_FAIL_URL_MSG if kind == "url" else SOURCE_FAIL_TEXT_MSG
    try:
        with_retry(retries=1, fn=lambda: nlm(p, "source", "add", "--notebook", nid, "--type", "url",
                                             "--timeout", "180", "--", value, timeout=360))
    except AuthError as e:
        # "Failed to add source" / rpc_code=9 -- bu manbaning o'zi ochilmagani, akkaunt auth xatosi emas
        if is_source_failure(str(e)):
            raise SourceError(fail_msg, f"source add ({kind}, «{title}»): {e}")
        raise
    except SourceError:
        raise
    except NlmError as e:
        raise SourceError(fail_msg, f"source add ({kind}, «{title}»): {e}")


def add_urls_batch(p: str, nid: str, urls: List[str]) -> None:
    """Barcha havolalarni bitta so'rovda navbatga qo'yadi (ingest tugashini wait_sources_ready kutadi)."""
    nlm(p, "source", "add-async", "--notebook", nid, "--", *urls, timeout=300)


def source_statuses(p: str, nid: str) -> List[str]:
    """`source list` jadvalidan har bir manbaning holatini (oxirgi ustun) o'qiydi."""
    out = nlm(p, "source", "list", "--notebook", nid, timeout=60)
    res = []
    for line in out.splitlines():
        if "│" not in line:
            continue
        cells = [c.strip() for c in line.split("│") if c.strip()]
        if len(cells) >= 2:
            res.append(cells[-1].lower())
    return res


def wait_sources_ready(p: str, nid: str, max_wait: int = 180) -> None:
    """Manbalar tayyor bo'lguncha kutadi; xato holatdagi manba bo'lsa, ishni to'xtatadi.
    `source list` o'qib bo'lmasa, 3 marta qayta urinadi — xatolik bilan davom etmaydi."""
    deadline = time.time() + max_wait
    read_errors = 0
    while True:
        try:
            st = source_statuses(p, nid)
            read_errors = 0
        except AuthError:
            raise
        except NlmError as e:
            read_errors += 1
            print(f"[wait_sources] {p}: holatni o'qib bo'lmadi ({read_errors}/3): {e}")
            if read_errors >= 3:
                raise SourceError(SOURCE_FAIL_TEXT_MSG, f"source list 3 marta xato: {e}")
            time.sleep(5)
            continue
        if any(s in BAD_STATUSES for s in st):
            raise SourceError(SOURCE_FAIL_URL_MSG if any("url" in s for s in st) else SOURCE_FAIL_TEXT_MSG,
                              f"manba holati xato: {st}")
        if not any(s in PENDING_STATUSES for s in st):
            return
        if time.time() > deadline:
            raise SourceError(SOURCE_FAIL_TEXT_MSG, f"manbalar {max_wait}s ichida tayyor bo'lmadi: {st}")
        time.sleep(5)


# ───────────────────────── KUTUBXONA ORQALI QO'SHISH + KUTISH (jarayon ichida, CLI'siz) ─────────────────────────
# NLM_SOURCE_LIB=0 -> eski CLI yo'liga qaytish. Batch bitta so'rovda yuboriladi, har havolaning holati alohida qaytadi
# (CONFIRMED / REJECTED / UNKNOWN / NOT_SENT), keyin tasdiqlangan manbalar ID'lari bilan BITTA wait_for_sources.
LIB_SOURCES = os.environ.get("NLM_SOURCE_LIB", "1").strip() != "0"


class LibUnavailable(Exception):
    """Kutubxona yo'li ishlamadi VA hech narsa yuborilmagan -> eski CLI yo'liga xavfsiz o'tish mumkin."""


class LibWaitFailed(NlmError):
    """Havolalar yuborilgan, lekin kutish kutilmagan xato berdi -> qayta QO'SHMANG, faqat `source list` bilan kuting."""


def _lib_exc(e: Exception) -> Exception:
    low = str(e).lower()
    if any(w in low for w in AUTH_WORDS):
        return AuthError(str(e)[:500])
    return NlmError(str(e)[:500])


async def _lib_run(nid: str, urls: List[str], max_wait: int, state: dict, log) -> dict:
    from notebooklm import NotebookLMClient      # ImportError -> LibUnavailable
    kw = {"profile": P}
    if LIGHT_BACKEND:
        kw["backend"] = LIGHT_BACKEND
    async with await NotebookLMClient.from_storage(**kw) as client:
        pending, ids, rejected, unknown = list(urls), [], [], []
        for attempt in (1, 2):                    # NOT_SENT (aniq yuborilmagan) bo'lsa, faqat o'shalarni bir marta qayta yuboramiz
            outs = await client.sources.add_urls_batch(nid, pending)
            state["sent"] = True
            pending = []
            for o in outs:
                st = o.outcome.commit_state.name
                if st == "CONFIRMED":
                    ids.append(o.source.id)
                elif st == "REJECTED":
                    rejected.append(o.url)
                elif st == "NOT_SENT":
                    pending.append(o.url)
                else:                             # UNKNOWN: qayta yubormaymiz (dublikat bo'lmasin), pastda source list tekshiradi
                    unknown.append(o.url)
            if not pending:
                break
        log(f"batch: tasdiqlangan={len(ids)} rad={len(rejected)} noma'lum={len(unknown)} yuborilmagan={len(pending)}")
        if rejected or pending:
            raise SourceError(SOURCE_FAIL_URL_MSG, f"havolalar qabul qilinmadi: rad={rejected} yuborilmagan={pending}")
        if ids:
            try:
                await client.sources.wait_for_sources(nid, ids, timeout=max_wait)
            except Exception as e:  # noqa: BLE001
                if "timeout" in type(e).__name__.lower() or "timed out" in str(e).lower():
                    raise SourceError(SOURCE_FAIL_TEXT_MSG, f"manbalar {max_wait}s ichida tayyor bo'lmadi: {e}")
                if any(w in str(e).lower() for w in AUTH_WORDS):
                    raise AuthError(str(e)[:500])
                raise LibWaitFailed(str(e)[:500])
        return {"confirmed": len(ids), "unknown": len(unknown)}


def lib_add_and_wait(nid: str, urls: List[str], max_wait: int, log) -> dict:
    state = {"sent": False}
    try:
        return asyncio.run(_lib_run(nid, urls, max_wait, state, log))
    except (SourceError, AuthError, LibWaitFailed):
        raise
    except Exception as e:  # noqa: BLE001
        if not state["sent"] and isinstance(e, (ImportError, AttributeError, TypeError, NotImplementedError)):
            raise LibUnavailable(str(e))
        if state["sent"]:
            raise LibWaitFailed(str(e)[:500])
        raise _lib_exc(e)



class AddReq(BaseModel):
    notebook_id: str
    urls: List[str] = Field(min_length=1, max_length=MAX_SOURCES)


def do_add(t: dict) -> dict:
    nid, urls, t0 = t["nid"], t["urls"], t["created"]
    if MAX_URLS and len(urls) > MAX_URLS:
        step(t["id"], f"havolalar {MAX_URLS} tagacha qisqartirildi ({len(urls)} -> {MAX_URLS})", t0)
        urls = urls[:MAX_URLS]
    step(t["id"], f"havola ID'lari: {[u.rsplit('/', 1)[-1] for u in urls]}", t0)
    max_wait = 300 + 30 * len(urls)
    if LIB_SOURCES:
        try:
            res = lib_add_and_wait(nid, urls, max_wait, lambda m: step(t["id"], m, t0))
            if res["unknown"]:                    # holati noma'lum havolalar bor -> bir marta `source list` bilan tekshiramiz
                step(t["id"], f"{res['unknown']} ta havola holati noma'lum, source list bilan tekshirilyapti", t0)
                wait_sources_ready(P, nid, max_wait=max_wait)
            step(t["id"], "manbalar tayyor", t0)
            return {"notebook_id": nid, "sources": len(urls)}
        except LibUnavailable as e:
            step(t["id"], f"kutubxona yo'li ishlamadi, CLI'ga o'tildi: {str(e)[:120]}", t0)
        except LibWaitFailed as e:
            step(t["id"], f"kutubxona kutishi xato berdi, source list bilan kutiladi: {str(e)[:120]}", t0)
            wait_sources_ready(P, nid, max_wait=max_wait)
            step(t["id"], "manbalar tayyor", t0)
            return {"notebook_id": nid, "sources": len(urls)}
    batched = False
    if BATCH_URLS and len(urls) >= 2:
        try:
            step(t["id"], f"{len(urls)} ta havola bitta so'rovda qo'shilyapti", t0)
            add_urls_batch(P, nid, urls)
            batched = True
        except AuthError:
            raise
        except NlmError as e:
            step(t["id"], f"ommaviy qo'shish xato berdi, birma-bir qo'shishga o'tildi: {str(e)[:120]}", t0)
    if not batched:
        for i, u in enumerate(urls, 1):
            step(t["id"], f"manba {i}/{len(urls)} qo'shilyapti", t0)
            add_source(P, nid, Source.model_construct(title="Manba", content=None, url=u))
    step(t["id"], "manbalar qo'shildi, indekslanishi kutilyapti", t0)
    wait_sources_ready(P, nid, max_wait=max_wait)
    step(t["id"], "manbalar tayyor", t0)
    return {"notebook_id": nid, "sources": len(urls)}


@app.post("/api/add", dependencies=[Depends(require_key)])
def add_endpoint(req: AddReq):
    """Havolalarni daftarga qo'shadi va indekslanishini kutadi. Darhol task_id qaytaradi; holat: GET /api/task/{id}."""
    nid = check_notebook_id(req.notebook_id)
    urls = []
    for u in req.urls:
        u = (u or "").strip()
        if not URL_ONLY_RE.match(u) or len(u) > 2000:
            raise HTTPException(422, "Havola noto'g'ri")
        if u not in urls:
            urls.append(u)
    for t in TASKS.values():     # bir daftarga ikki marta qo'shishni oldini olish (qayta yuborilgan so'rov)
        if t["kind"] == "add" and t.get("nid") == nid and t["status"] in ("queued", "running"):
            return {"task_id": t["id"], "status": t["status"], "duplicate": True}
    t = new_task("add", nid=nid, urls=urls)
    start_task(t, do_add)
    print(f"[create] s2 task={t['id'][:8]} urls={len(urls)}", flush=True)
    return {"task_id": t["id"], "status": "queued"}
