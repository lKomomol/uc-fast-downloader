#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UC 分享链接解析 / 浏览器登录态抓取（只用标准库）

对外主要接口：
    parse_share(text)                 -> (pwd_id, passcode)
    resolve_share(pwd_id, passcode, cookie_header="") -> dict
    login_via_browser(on_status=None) -> (cookies, info)
    CookieStore()                     -> 本地加密保存/读取登录态
"""

import base64
import ctypes
import ctypes.wintypes as wt
import http.cookiejar
import json
import os
import re
import socket
import ssl
import struct
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36")
API = "https://pc-api.uc.cn"
PARAMS = {"pr": "UCBrowser", "fr": "pc"}
LOGIN_PAGE = "https://drive.uc.cn/"

APP_DIR = os.path.join(os.environ.get("LOCALAPPDATA") or os.path.expanduser("~"), "UC下载器")
PROFILE_DIR = os.path.join(APP_DIR, "browser")
STORE_PATH = os.path.join(APP_DIR, "login.dat")
SETTINGS_PATH = os.path.join(APP_DIR, "settings.json")

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

BROWSERS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
]

FILE_NOT_FOUND = (41006,)
NEED_PASSCODE = (41008,)
BAD_PASSCODE = (41007,)
NEED_LOGIN = (31001, 401)


class ShareError(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class ShareCancelled(ShareError):
    """用户点了停止"""


class LoginError(Exception):
    pass


# ---------------------------------------------------------------- 基础工具

def parse_share(text):
    """从分享链接/分享文本里取 (pwd_id, passcode)"""
    text = (text or "").strip()
    m = re.search(r"(?:drive|fast)\.uc\.cn/s/([0-9A-Za-z]+)", text)
    if not m:
        m = re.fullmatch(r"([0-9A-Za-z]{8,})", text)
    if not m:
        raise ShareError("没找到分享链接（应该是 https://drive.uc.cn/s/xxxx 这种）")
    pwd_id = m.group(1)
    code = ""
    for pat in (r"[?&#]pwd=([0-9A-Za-z]+)", r"提取码[：:\s]*([0-9A-Za-z]{4,10})"):
        mm = re.search(pat, text)
        if mm:
            code = mm.group(1)
            break
    return pwd_id, code


def looks_like_share(text):
    text = (text or "").strip()
    return bool(re.search(r"(?:drive|fast)\.uc\.cn/s/", text)) or bool(re.fullmatch(r"[0-9A-Za-z]{8,}", text))


def sanitize(name):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name or "").strip(" .")
    return name or "download.mp4"


def _cookie(name, value, domain=".uc.cn", path="/"):
    return http.cookiejar.Cookie(
        version=0, name=name, value=value, port=None, port_specified=False,
        domain=domain, domain_specified=True, domain_initial_dot=domain.startswith("."),
        path=path, path_specified=True, secure=True, expires=None, discard=False,
        comment=None, comment_url=None, rest={}, rfc2109=False)


def _seed_jar(header):
    jar = http.cookiejar.CookieJar()
    for part in (header or "").split(";"):
        name, _, value = part.strip().partition("=")
        if name:
            jar.set_cookie(_cookie(name, value))
    return jar


def _opener(jar):
    return urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=SSL_CTX),
        urllib.request.HTTPCookieProcessor(jar))


def _now():
    return int(time.time())


def _cookie_alive(c):
    exp = c.get("expires")
    return not exp or exp < 0 or exp > _now()


def _jar_header(jar):
    """把 jar 里的 uc.cn Cookie 拼成请求头（同名去重，优先 .uc.cn / 更长 path）"""
    best = {}
    for c in jar:
        if not str(c.domain).lstrip(".").endswith("uc.cn"):
            continue
        exp = getattr(c, "expires", None)
        if exp and exp > 0 and exp <= _now():
            continue
        score = (str(c.domain) == ".uc.cn", len(str(c.path or "/")))
        if c.name not in best or score > best[c.name][0]:
            best[c.name] = (score, c)
    return "; ".join(f"{c.name}={c.value}" for _, c in best.values())


def _api(opener, path, method="GET", body=None, params=None, timeout=15):
    url = API + path + "?" + urllib.parse.urlencode(PARAMS)
    if params:
        url += "&" + urllib.parse.urlencode(params)
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://drive.uc.cn",
        "Referer": "https://drive.uc.cn/",
    }
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with opener.open(req, timeout=timeout) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        raw = e.read()
    except Exception as e:
        raise ShareError(f"网络请求失败：{type(e).__name__}: {str(e)[:80]}")
    try:
        return json.loads(raw)
    except Exception:
        raise ShareError("接口返回的不是 JSON：" + raw[:120].decode("utf-8", "ignore"))


def _speed_mode(url):
    """从 callback-var 的 x:token 判断这次是登录态（500）还是游客（100）"""
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        cv = qs.get("callback-var", [""])[0]
        pad = cv + "=" * (-len(cv) % 4)
        d = json.loads(base64.urlsafe_b64decode(pad))
        token = d.get("x:token", "")
        if "-500-" in token:
            return "login"
        if "-100-" in token:
            return "guest"
    except Exception:
        pass
    return "unknown"


# ---------------------------------------------------------------- 分享解析
# 流程：token -> detail -> file/download
# token 接口不需要登录；带登录 Cookie 时拿到的直链单连接约 500KB/s，游客约 100KB/s

def resolve_share(pwd_id, passcode="", cookie_header="", on_step=None, cancel=None):
    def step(msg):
        if on_step:
            try:
                on_step(msg)
            except Exception:
                pass

    def check():
        if cancel is not None and cancel.is_set():
            raise ShareCancelled("已取消解析")

    jar = _seed_jar(cookie_header)
    opener = _opener(jar)

    check()
    step("取分享令牌…")
    tok = _api(opener, "/1/clouddrive/share/sharepage/token", "POST",
               {"pwd_id": pwd_id, "passcode": passcode or ""})
    check()
    code = tok.get("code")
    if code in NEED_PASSCODE:
        raise ShareError("这个分享需要提取码，请在「提取码」里填上", code)
    if code in BAD_PASSCODE:
        raise ShareError("提取码不对，改一下再试", code)
    if code in FILE_NOT_FOUND:
        raise ShareError("分享不存在或已经失效", code)
    stoken = (tok.get("data") or {}).get("stoken")
    if not stoken:
        raise ShareError(tok.get("message") or "取不到分享令牌", code)

    p = {
        "pwd_id": pwd_id, "stoken": stoken, "pdir_fid": "0", "force": "0",
        "_page": "1", "_size": "100", "_fetch_banner": "1", "_fetch_share": "1",
        "_fetch_total": "1", "_sort": "file_type:asc,updated_at:desc",
    }
    step("读取文件列表…")
    detail = _api(opener, "/1/clouddrive/share/sharepage/detail", params=p)
    check()
    if detail.get("code") not in (0, None):
        raise ShareError(detail.get("message") or "读取分享列表失败", detail.get("code"))
    data = detail.get("data") or {}
    items = data.get("list") or []
    files = [x for x in items if not x.get("dir")]
    if not files:
        raise ShareError("这个分享里没有可下载的文件（文件夹分享暂不支持）")

    f = files[0]
    step(f"找到 {f.get('file_name') or '文件'}，正在取直链…")
    dl_body = {
        "fids": [f["fid"]],
        "pwd_id": pwd_id,
        "stoken": stoken,
        "fids_token": [f.get("share_fid_token", "")],
    }
    dl = _api(opener, "/1/clouddrive/file/download", "POST", dl_body)
    check()

    # 登录态失效时自动退回游客再试一次
    if dl.get("code") in NEED_LOGIN and cookie_header:
        jar = http.cookiejar.CookieJar()
        opener = _opener(jar)
        step("登录态失效，改用游客重试…")
        dl = _api(opener, "/1/clouddrive/file/download", "POST", dl_body)
        check()

    entries = dl.get("data") or []
    if isinstance(entries, dict):
        entries = [entries]
    entry = entries[0] if entries else None
    url = (entry or {}).get("download_url")
    if not url:
        raise ShareError(dl.get("message") or "还没拿到下载直链（文件可能正在打包）", dl.get("code"))

    return {
        "url": url,
        "cookie": _jar_header(jar),
        "referer": "https://drive.uc.cn/",
        "ua": UA,
        "filename": sanitize(entry.get("file_name") or f.get("file_name") or data.get("share", {}).get("title")),
        "size": entry.get("size") or f.get("size"),
        "mode": _speed_mode(url),
        "files": [
            {"fid": x.get("fid"), "name": x.get("file_name"), "size": x.get("size"),
             "dir": bool(x.get("dir"))} for x in items
        ],
        "title": data.get("share", {}).get("title"),
    }


# ---------------------------------------------------------------- 本地设置

def load_settings():
    try:
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_settings(data):
    try:
        os.makedirs(APP_DIR, exist_ok=True)
        tmp = SETTINGS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        os.replace(tmp, SETTINGS_PATH)
    except Exception:
        pass


# ---------------------------------------------------------------- 登录态存储

class _BLOB(ctypes.Structure):
    _fields_ = [("cbData", wt.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]


def _dpapi(data, protect=True):
    buf = ctypes.create_string_buffer(data, len(data))
    blob_in = _BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = _BLOB()
    fn = ctypes.windll.crypt32.CryptProtectData if protect else ctypes.windll.crypt32.CryptUnprotectData
    fn.argtypes = [ctypes.POINTER(_BLOB), ctypes.c_void_p, ctypes.POINTER(_BLOB),
                   ctypes.c_void_p, ctypes.c_void_p, wt.DWORD, ctypes.POINTER(_BLOB)]
    fn.restype = wt.BOOL
    if not fn(ctypes.byref(blob_in), None, None, None, None, 0x1, ctypes.byref(blob_out)):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)


class CookieStore:
    """登录态用 Windows DPAPI 加密后存在 %LOCALAPPDATA%\\UC下载器\\login.dat"""

    def __init__(self, path=STORE_PATH):
        self.path = path
        self._cache = None
        self._loaded = False

    def _load(self):
        if self._loaded:
            return self._cache
        self._loaded = True
        try:
            raw = open(self.path, "rb").read()
            self._cache = json.loads(_dpapi(raw, protect=False).decode("utf-8"))
        except Exception:
            self._cache = None
        return self._cache

    def header(self):
        d = self._load()
        return (d or {}).get("header", "")

    def info(self):
        d = self._load()
        return (d or {}).get("info") or None

    def saved_at(self):
        d = self._load()
        return (d or {}).get("saved_at", 0)

    def save(self, cookies, info=None):
        keep = []
        for c in cookies or []:
            if not str(c.get("domain", "")).lstrip(".").endswith("uc.cn"):
                continue
            if not _cookie_alive(c):
                continue
            keep.append({k: c.get(k) for k in ("name", "value", "domain", "path", "expires", "httpOnly", "secure")})
        best = {}
        for c in keep:
            score = (c.get("domain") == ".uc.cn", len(c.get("path") or "/"))
            if c["name"] not in best or score > best[c["name"]][0]:
                best[c["name"]] = (score, c)
        header = "; ".join(f"{c['name']}={c['value']}" for _, c in best.values())
        payload = {
            "cookies": keep,
            "header": header,
            "info": info or {},
            "saved_at": _now(),
        }
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        blob = _dpapi(json.dumps(payload, ensure_ascii=False).encode("utf-8"), protect=True)
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(blob)
        os.replace(tmp, self.path)
        self._cache = payload
        self._loaded = True
        return payload

    def clear(self):
        self._cache = None
        self._loaded = True
        try:
            os.remove(self.path)
        except OSError:
            pass


# ---------------------------------------------------------------- 极简 WebSocket（CDP 用）

class _WS:
    def __init__(self, url, timeout=10):
        rest = url[5:]
        hostport, _, path = rest.partition("/")
        host, _, port = hostport.partition(":")
        self.s = socket.create_connection((host, int(port or 80)), timeout=timeout)
        self.s.settimeout(timeout)
        self.buf = b""
        self._id = 0
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET /{path} HTTP/1.1\r\nHost: {hostport}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
               f"Sec-WebSocket-Version: 13\r\nOrigin: http://127.0.0.1\r\n\r\n")
        self.s.sendall(req.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.s.recv(4096)
            if not chunk:
                raise LoginError("登录窗口的连接断开了")
            head += chunk
        head, _, self.buf = head.partition(b"\r\n\r\n")
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise LoginError("无法连接浏览器的调试端口")

    def _exact(self, n):
        while len(self.buf) < n:
            chunk = self.s.recv(max(4096, n - len(self.buf)))
            if not chunk:
                raise LoginError("登录窗口已关闭")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _frame(self, opcode, data=b""):
        n = len(data)
        header = bytearray([0x80 | opcode])
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack("!H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", n)
        mask = os.urandom(4)
        header += mask
        self.s.sendall(bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def recv(self):
        chunks = []
        while True:
            b1, b2 = self._exact(2)
            fin, opcode = b1 & 0x80, b1 & 0x0F
            masked, n = b2 & 0x80, b2 & 0x7F
            if n == 126:
                n = struct.unpack("!H", self._exact(2))[0]
            elif n == 127:
                n = struct.unpack("!Q", self._exact(8))[0]
            mask = self._exact(4) if masked else None
            data = self._exact(n)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if opcode == 0x9:
                self._frame(0xA, data)
                continue
            if opcode == 0x8:
                raise LoginError("登录窗口已关闭")
            if opcode in (0x1, 0x2):
                chunks = [data]
            elif opcode == 0x0:
                chunks.append(data)
            if fin and chunks:
                return b"".join(chunks).decode("utf-8", "ignore")

    def call(self, method, params=None):
        self._id += 1
        mid = self._id
        self._frame(0x1, json.dumps({"id": mid, "method": method, "params": params or {}}).encode())
        while True:
            msg = json.loads(self.recv())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise LoginError(str(msg["error"].get("message") or msg["error"]))
                return msg.get("result", {})

    def close(self):
        try:
            self.s.close()
        except Exception:
            pass


# ---------------------------------------------------------------- 浏览器登录

def find_browser():
    for p in BROWSERS:
        if os.path.exists(p):
            return p
    raise LoginError("没找到 Chrome / Edge，请先装一个浏览器")


def _port_ok(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except Exception:
        return False


def _read_devtools(profile, deadline):
    path = os.path.join(profile, "DevToolsActivePort")
    while time.time() < deadline:
        try:
            parts = open(path).read().split()
            if len(parts) >= 2 and _port_ok(int(parts[0])):
                return int(parts[0]), parts[1]
        except Exception:
            pass
        time.sleep(0.25)
    raise LoginError("登录窗口启动超时")


def _targets(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=5) as r:
            return json.loads(r.read())
    except Exception:
        return []


def _page_ws(port):
    pages = [t for t in _targets(port) if t.get("type") == "page"]
    pages.sort(key=lambda t: 0 if "drive.uc.cn" in t.get("url", "") else 1)
    for t in pages:
        if t.get("webSocketDebuggerUrl"):
            return t["webSocketDebuggerUrl"].replace("ws://localhost", "ws://127.0.0.1")
    return None


_MEMBER_JS = ("(async()=>{try{const r=await fetch('https://pc-api.uc.cn/1/clouddrive/member"
              "?pr=UCBrowser&fr=pc',{credentials:'include',headers:{'Accept':'application/json, text/plain, */*'}});"
              "return r.status+'|'+await r.text();}catch(e){return '0|'+e;}})()")


def _member_status(port):
    ws_url = _page_ws(port)
    if not ws_url:
        return None
    ws = _WS(ws_url, timeout=10)
    try:
        r = ws.call("Runtime.evaluate",
                    {"expression": _MEMBER_JS, "awaitPromise": True, "returnByValue": True})
        val = (r.get("result") or {}).get("value") or ""
        if "|" not in val:
            return None
        status, body = val.split("|", 1)
        try:
            data = json.loads(body)
        except Exception:
            data = {}
        return int(status), data
    finally:
        ws.close()


def login_via_browser(on_status=None, timeout=300, profile_dir=PROFILE_DIR):
    """弹一个干净的浏览器窗口让用户自己登录（手机号+验证码/扫码都行）

    登录成功返回 (cookies, info)；cookies 是 CDP 读到的浏览器 Cookie 列表。
    """
    def say(msg):
        if on_status:
            try:
                on_status(msg)
            except Exception:
                pass

    os.makedirs(profile_dir, exist_ok=True)
    exe = find_browser()
    port_file = os.path.join(profile_dir, "DevToolsActivePort")
    proc = None
    port = None
    ws_path = None

    # 复用已经开着调试端口的窗口
    try:
        parts = open(port_file).read().split()
        if len(parts) >= 2 and _port_ok(int(parts[0])):
            port, ws_path = int(parts[0]), parts[1]
    except Exception:
        pass

    if not port:
        try:
            os.remove(port_file)
        except OSError:
            pass
        proc = subprocess.Popen(
            [exe, f"--user-data-dir={profile_dir}", "--remote-debugging-port=0",
             "--remote-allow-origins=*", "--no-first-run", "--no-default-browser-check",
             "--disable-sync", "--disable-features=Translate", f"--app={LOGIN_PAGE}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        say("已打开登录窗口，请在窗口里点「登录」→「短信验证」，输手机号和验证码")
        try:
            port, ws_path = _read_devtools(profile_dir, time.time() + 40)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
            raise

    try:
        bws = _WS(f"ws://127.0.0.1:{port}{ws_path}", timeout=20)
    except Exception:
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
        raise
    deadline = time.time() + timeout
    last_note = 0
    try:
        while time.time() < deadline:
            time.sleep(1.2)
            try:
                res = bws.call("Storage.getCookies")
                cookies = res.get("cookies") or []
            except LoginError:
                raise
            uc = [c for c in cookies if str(c.get("domain", "")).lstrip(".").endswith("uc.cn")]
            if uc:
                try:
                    got = _member_status(port)
                except LoginError:
                    raise
                except Exception:
                    got = None
                if got and got[0] == 200:
                    data = (got[1].get("data") or {}) if isinstance(got[1], dict) else {}
                    info = {
                        "member_type": data.get("member_type"),
                        "created_at": data.get("created_at"),
                        "is_new_user": data.get("is_new_user"),
                    }
                    try:
                        cookies = bws.call("Storage.getCookies").get("cookies") or cookies
                    except Exception:
                        pass
                    say("登录成功")
                    return cookies, info
            if time.time() - last_note > 30:
                say("等待登录…（在窗口里完成手机号+验证码即可）")
                last_note = time.time()
        raise LoginError("登录超时了，重新点一次「登录」吧")
    finally:
        try:
            bws.call("Browser.close")
        except Exception:
            pass
        try:
            bws.close()
        except Exception:
            pass
        if proc is not None:
            try:
                proc.wait(timeout=6)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "login":
        store = CookieStore()
        cs, info = login_via_browser(print)
        store.save(cs, info)
        print("saved", len(cs), "cookies ->", store.path)
    elif len(sys.argv) >= 2:
        pwd_id, pwd = parse_share(sys.argv[1])
        code = sys.argv[2] if len(sys.argv) > 2 else pwd
        store = CookieStore()
        r = resolve_share(pwd_id, code, store.header())
        print(json.dumps({k: v for k, v in r.items() if k != "url"}, ensure_ascii=False, indent=1))
        print("url:", r["url"][:160], "...")
