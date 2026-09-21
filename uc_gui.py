#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UC 多线程下载器（图形界面版）

用法:
    python uc_gui.py                # 打开界面
    python uc_gui.py --selftest     # 自检：自动跑一小段下载并退出

使用方式：
    在 UC 网页点下载后，按 F12 -> Network -> 右键那条请求 -> Copy as cURL (bash)，
    把整段内容粘进窗口，点「解析并开始下载」。
"""

import json
import os
import queue
import re
import shutil
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

APP_TITLE = "UC 多线程下载器"
if getattr(sys, "frozen", False):          # PyInstaller 单文件模式：用 exe 所在目录
    HERE = os.path.dirname(os.path.abspath(sys.executable))
else:
    HERE = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
    ),
}
DEFAULT_REFERER = "https://fast.uc.cn/"


# ---------------------------------------------------------------- cURL 解析

def _normalize(text):
    """去掉 bash/cmd 的续行符，压成一行"""
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\\\s*\n", " ", t)          # bash: \ <换行>
    t = re.sub(r"\^\s*\n", " ", t)          # cmd:  ^ <换行>
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _score_url(u):
    s = 0
    if "Signature=" in u or "callback-var=" in u:
        s += 4
    if "OSSAccessKeyId=" in u:
        s += 2
    if "pds.uc.cn" in u or "/dl-" in u:
        s += 2
    if u.endswith(".mp4") or "response-content-disposition" in u:
        s += 1
    return s


def parse_curl(text):
    """从 Copy as cURL 的文本里抽出 url / cookie / referer / ua"""
    t = _normalize(text)

    url = None
    m = re.search(r"--url\s+(['\"])(.*?)\1", t)
    if m:
        url = m.group(2)
    if not url:
        cands = [c[1] for c in re.findall(r"(['\"])(https?://[^'\"]+?)\1", t)]
        cands = [c for c in cands if "//" in c]
        if cands:
            url = max(cands, key=_score_url)
    if not url:
        m = re.search(r"(https?://\S+)", t)
        if m:
            url = m.group(1).strip("'\"")
    if not url:
        raise ValueError("没找到下载链接（http/https）")

    headers = {}
    for _, name, value in re.findall(r"(?:-H|--header)\s+(['\"])([^:]+):\s*(.*?)\1", t):
        headers[name.strip().lower()] = value.strip()

    cookie = headers.get("cookie")
    if not cookie:
        m = re.search(r"(?:-b|--cookie)\s+(['\"])(.*?)\1", t)
        if m:
            cookie = m.group(2)

    referer = headers.get("referer") or DEFAULT_REFERER
    ua = headers.get("user-agent") or DEFAULT_HEADERS["User-Agent"]

    return {"url": url, "cookie": cookie or "", "referer": referer, "ua": ua}


def filename_from_url(url):
    """尽量从直链的 response-content-disposition 里还原真实文件名"""
    try:
        qs = urllib.parse.urlparse(url).query
        params = urllib.parse.parse_qs(qs)
        cd = params.get("response-content-disposition", [""])[0]
        disp = urllib.parse.unquote(cd)          # 第一层：%3B -> ;
        m = re.search(r"filename=([^;]+)", disp, re.I)
        if m:
            name = m.group(1).strip().strip("'\"")
            name = urllib.parse.unquote(name)    # 第二层：%28 -> (
            if name:
                return sanitize(name)
        m = re.search(r"filename\*=[^']*''([^;]+)", disp, re.I)
        if m:
            return sanitize(urllib.parse.unquote(m.group(1)))
    except Exception:
        pass
    path = urllib.parse.urlparse(url).path
    base = os.path.basename(path)
    return sanitize(base) if base else "download.mp4"


def sanitize(name):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")
    return name or "download.mp4"


def build_headers(parsed):
    h = dict(DEFAULT_HEADERS)
    h["Referer"] = parsed.get("referer") or DEFAULT_REFERER
    h["User-Agent"] = parsed.get("ua") or DEFAULT_HEADERS["User-Agent"]
    if parsed.get("cookie"):
        h["Cookie"] = parsed["cookie"]
    return h


def http_hint(err):
    """把 HTTP 错误翻译成人话"""
    code = getattr(err, "code", None)
    body = ""
    try:
        body = err.read()[:400].decode("utf-8", "ignore")
    except Exception:
        pass
    if "require login" in body or "auth not found" in body:
        return ("403：UC 登录校验没通过（Cookie 缺失或已失效）。\n"
                "     请回网页重新点下载 → F12 → Network → 右键该请求 → Copy as cURL，再粘进来。")
    if "SignatureDoesNotMatch" in body or code == 403:
        return ("403：直链已过期或被拒绝（直链有效期约 3 小时）。\n"
                "     请回网页重新点下载，重新 Copy as cURL 粘贴，可继续断点续传。")
    return f"HTTP {code}: {body[:200]}"


# ---------------------------------------------------------------- 下载核心

class Downloader:
    """多线程分块下载，事件通过队列发给界面"""

    def __init__(self, parsed, out_path, conns, chunk_bytes, q, stop_event):
        self.parsed = parsed
        self.headers = build_headers(parsed)
        self.url = parsed["url"]
        self.out_path = out_path
        self.state_path = out_path + ".parts.json"
        self.conns = max(1, int(conns))
        self.chunk = max(262144, int(chunk_bytes))
        self.q = q
        self.stop = stop_event
        self.total = None
        self.fh = None
        self.done = set()
        self.pending = []
        self.done_bytes = 0      # 已落盘（完整分块）字节数
        self.recv_bytes = 0      # 已接收字节数（含正在下的分块，给进度条用）
        self.fatal = None
        self._lock = threading.Lock()

    # -- 网络 --
    def _open(self, off=None, end=None, method=None, timeout=45):
        h = dict(self.headers)
        if off is not None:
            h["Range"] = f"bytes={off}-{end}"
        req = urllib.request.Request(self.url, headers=h, method=method)
        return urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)

    def _head(self):
        # 注意：UC 的回调只放行 GET（HEAD 会被判 403 RequestDeniedByCallback），
        # 所以这里用 Range: bytes=0-0 的 GET 来探测文件大小。
        with self._open(0, 0) as r:
            cr = r.headers.get("Content-Range", "")
            if "/" in cr:
                return int(cr.split("/")[-1])
            if r.headers.get("Content-Length"):
                return int(r.headers["Content-Length"])
        raise IOError("服务端没有返回文件大小")

    def _give_back(self, n):
        """本次尝试失败/被中断时，把已计入的字节退回去，避免进度虚高"""
        if n:
            with self._lock:
                self.recv_bytes -= n

    def _fetch(self, off, end, tries=6):
        want = end - off + 1
        for attempt in range(tries):
            if self.stop.is_set() or self.fatal:
                return None
            got = 0
            try:
                with self._open(off, end) as r:
                    if r.status not in (200, 206):
                        raise IOError(f"HTTP {r.status}")
                    buf = bytearray()
                    while len(buf) < want:
                        if self.stop.is_set():
                            self._give_back(got)
                            return None
                        b = r.read(min(262144, want - len(buf)))
                        if not b:
                            break
                        buf += b
                        got += len(b)
                        with self._lock:
                            self.recv_bytes += len(b)
                    if len(buf) != want:
                        raise IOError(f"数据不完整 {len(buf)}/{want}")
                    return bytes(buf)
            except urllib.error.HTTPError as e:
                self._give_back(got)
                if e.code == 403:
                    self.fatal = http_hint(e)
                    return None
                if attempt == tries - 1:
                    self.q.put(("log", f"分块 {off//self.chunk} 失败：HTTP {e.code}", "err"))
            except Exception as e:
                self._give_back(got)
                if self.stop.is_set():
                    return None
                if attempt == tries - 1:
                    self.q.put(("log", f"分块 {off//self.chunk} 重试 {tries} 次仍失败：{type(e).__name__}: {str(e)[:70]}", "err"))
            time.sleep(min(0.5 * (2 ** attempt), 5))
        return None

    # -- 状态 --
    def _save_state(self):
        try:
            tmp = self.state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"total": self.total, "chunk": self.chunk, "done": sorted(self.done)}, f)
            os.replace(tmp, self.state_path)
        except Exception:
            pass

    def _worker(self):
        while not self.stop.is_set() and not self.fatal:
            with self._lock:
                if not self.pending:
                    return
                idx = self.pending.pop(0)
            off = idx * self.chunk
            end = min(off + self.chunk, self.total) - 1
            data = self._fetch(off, end)
            if data is None:
                if not self.stop.is_set() and not self.fatal:
                    with self._lock:
                        self.pending.insert(0, idx)
                return
            with self._lock:
                self.fh.seek(off)
                self.fh.write(data)
                self.done.add(idx)
                self.done_bytes += len(data)
                self._save_state()

    # -- 主流程 --
    def run(self):
        q = self.q
        try:
            self.total = self._head()
        except urllib.error.HTTPError as e:
            q.put(("failed", http_hint(e)))
            return
        except Exception as e:
            q.put(("failed", f"获取文件信息失败：{type(e).__name__}: {str(e)[:120]}"))
            return

        nchunks = (self.total + self.chunk - 1) // self.chunk
        q.put(("log", f"文件大小 {self.total/1048576:.1f} MB，分 {nchunks} 块（{self.chunk//1048576} MB/块），{min(self.conns, nchunks)} 线程", "ok"))

        resume = False
        if os.path.exists(self.state_path):
            try:
                st = json.load(open(self.state_path, encoding="utf-8"))
                if st.get("total") == self.total and st.get("chunk") == self.chunk:
                    self.done = set(st.get("done", []))
                    resume = bool(self.done)
                    if resume:
                        self.done_bytes = sum(
                            min(self.chunk, self.total - i * self.chunk) for i in self.done
                        )
                        self.recv_bytes = self.done_bytes
                        q.put(("log", f"发现上次未完成的进度：已完成 {len(self.done)}/{nchunks} 块，继续续传", "warn"))
            except Exception:
                pass

        os.makedirs(os.path.dirname(self.out_path) or ".", exist_ok=True)
        self.fh = open(self.out_path, "r+b" if resume else "wb")
        self.fh.truncate(self.total)

        self.pending = [i for i in range(nchunks) if i not in self.done]
        if not self.pending:
            self.fh.close()
            q.put(("progress", self._progress(0.0, 0.0)))
            q.put(("done", {"path": self.out_path, "total": self.total, "seconds": 0.0, "resumed": True}))
            return

        t0 = time.time()
        last = [t0, self.recv_bytes]
        ths = [threading.Thread(target=self._worker, daemon=True)
               for _ in range(min(self.conns, len(self.pending)))]
        for t in ths:
            t.start()

        while any(t.is_alive() for t in ths):
            time.sleep(0.35)
            now = time.time()
            cur = self.recv_bytes
            if cur != last[1]:          # 用“已接收字节”驱动进度，开头就能动
                inst = (cur - last[1]) / max(now - last[0], 1e-6) / 1048576.0
                avg = cur / max(now - t0, 1e-6) / 1048576.0
                left = (self.total - cur) / max(inst * 1048576.0, 1.0)
                q.put(("progress", self._progress(inst, avg, left)))
                last = [now, cur]
            if self.fatal or self.stop.is_set():
                for t in ths:
                    t.join(timeout=8)
                break

        for t in ths:
            t.join(timeout=3)
        self._save_state()
        elapsed = time.time() - t0

        if self.fatal:
            self.fh.close()
            q.put(("failed", self.fatal))
            return
        if self.pending:
            self.fh.close()
            q.put(("failed", f"还有 {len(self.pending)} 块没下完（已停止）。已保存进度，重新粘贴新链接可续传。"))
            return

        self.fh.flush()
        self.fh.close()
        size = os.path.getsize(self.out_path)
        if size != self.total:
            q.put(("failed", f"大小校验不一致：磁盘 {size} / 预期 {self.total}"))
            return
        q.put(("log", "大小校验通过，正在检查是否有空洞…", "info"))
        zeros = self._scan_zeros()
        q.put(("log", f"空洞检查：全零块 {zeros} 个" + ("（正常）" if zeros == 0 else "（可能有缺失数据，建议重下）"),
               "ok" if zeros == 0 else "warn"))
        if os.path.exists(self.state_path):
            os.remove(self.state_path)
        self.recv_bytes = self.total
        q.put(("progress", self._progress(0.0, self.total / max(elapsed, 1e-6) / 1048576.0, 0.0)))
        q.put(("done", {"path": self.out_path, "total": self.total, "seconds": elapsed,
                        "resumed": resume, "zeros": zeros}))

    def _progress(self, inst, avg, left=0.0):
        return {"done": min(self.recv_bytes, self.total or 0), "total": self.total or 0,
                "inst": inst, "avg": avg,
                "left": left, "chunks_done": len(self.done),
                "chunks_total": (self.total + self.chunk - 1) // self.chunk if self.total else 0}

    def _scan_zeros(self):
        zeros = 0
        try:
            with open(self.out_path, "rb") as f:
                while True:
                    b = f.read(4 << 20)
                    if not b:
                        break
                    if not any(b):
                        zeros += 1
        except Exception:
            return -1
        return zeros


# ---------------------------------------------------------------- 界面

LOG_COLORS = {"info": "#222222", "ok": "#137333", "warn": "#b06000", "err": "#c5221f"}


class App:
    def __init__(self, root, out_dir=None, selftest=False):
        import tkinter as tk
        from tkinter import filedialog, font as tkfont, ttk

        self.tk = tk
        self.ttk = ttk
        self.filedialog = filedialog
        self.root = root
        self.q = queue.Queue()
        self.stop_event = threading.Event()
        self.worker = None
        self.dl = None
        self.selftest = selftest
        self.out_dir = out_dir or os.path.join(os.path.expanduser("~"), "Downloads")
        self.out_path = None

        root.title(APP_TITLE)
        root.geometry("760x580")
        root.minsize(680, 520)
        try:
            tkfont.nametofont("TkDefaultFont").configure(family="Microsoft YaHei UI", size=10)
            tkfont.nametofont("TkTextFont").configure(family="Microsoft YaHei UI", size=10)
        except Exception:
            pass

        pad = {"padx": 10, "pady": 4}

        # ① 粘贴区
        ttk.Label(root, text="① 粘贴 Copy as cURL 的整段内容：").pack(anchor="w", **pad)
        box = ttk.Frame(root)
        box.pack(fill="both", expand=True, padx=10)
        self.text = tk.Text(box, height=7, wrap="word", undo=True,
                            font=("Consolas", 9), relief="solid", borderwidth=1)
        sb = ttk.Scrollbar(box, command=self.text.yview)
        self.text.configure(yscrollcommand=sb.set)
        self.text.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # 按钮行
        bar = ttk.Frame(root)
        bar.pack(fill="x", **pad)
        self.btn_paste = ttk.Button(bar, text="从剪贴板粘贴", command=self.paste_clipboard)
        self.btn_start = ttk.Button(bar, text="解析并开始下载", command=self.start)
        self.btn_stop = ttk.Button(bar, text="停止", command=self.stop, state="disabled")
        self.btn_open = ttk.Button(bar, text="打开所在文件夹", command=self.open_folder)
        for w in (self.btn_paste, self.btn_start, self.btn_stop, self.btn_open):
            w.pack(side="left", padx=3)
        ttk.Label(bar, text="连接数").pack(side="left", padx=(14, 2))
        self.conns = ttk.Combobox(bar, width=4, values=("16", "32", "48", "64", "96"), state="readonly")
        self.conns.set("64")
        self.conns.pack(side="left")
        ttk.Label(bar, text="分块").pack(side="left", padx=(10, 2))
        self.chunk = ttk.Combobox(bar, width=6, values=("2 MB", "4 MB", "8 MB"), state="readonly")
        self.chunk.set("4 MB")
        self.chunk.pack(side="left")

        # ② 进度条
        ttk.Label(root, text="② 进度：").pack(anchor="w", **pad)
        self.pb = ttk.Progressbar(root, maximum=100.0, mode="determinate")
        self.pb.pack(fill="x", padx=10)
        self.lbl_size = ttk.Label(root, text="0.0 / 0.0 MB          瞬时 0.00 MB/s     均速 0.00 MB/s",
                                  font=("Consolas", 10))
        self.lbl_size.pack(anchor="w", **pad)
        self.lbl_chunk = ttk.Label(root, text="已完成 0/0 块          预计剩余 -- 秒",
                                   font=("Consolas", 10))
        self.lbl_chunk.pack(anchor="w", padx=10)
        self.lbl_path = ttk.Label(root, text=f"保存到：{self.out_dir}", foreground="#555555")
        self.lbl_path.pack(anchor="w", padx=10, pady=(6, 2))

        # ④ 日志区
        ttk.Label(root, text="④ 日志：").pack(anchor="w", **pad)
        lb = ttk.Frame(root)
        lb.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log = tk.Text(lb, height=8, wrap="word", state="disabled", relief="solid",
                           borderwidth=1, font=("Microsoft YaHei UI", 9))
        lsb = ttk.Scrollbar(lb, command=self.log.yview)
        self.log.configure(yscrollcommand=lsb.set)
        self.log.pack(side="left", fill="both", expand=True)
        lsb.pack(side="right", fill="y")
        for k, c in LOG_COLORS.items():
            self.log.tag_configure(k, foreground=c)

        self.msg("准备就绪。在 UC 网页点下载后，按 F12 → Network → 右键那条请求 → Copy as cURL (bash)，把整段粘进来。", "info")
        self.icon_loaded = self._load_icon()
        self._closing = False
        self._poll_id = self.root.after(400, self.poll)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # 剪贴板里如果已经有 curl 内容，自动填入
        try:
            clip = self.root.clipboard_get()
            if re.search(r"\bcurl\b", clip, re.I) and "http" in clip:
                self.text.insert("1.0", clip)
                self.msg("检测到剪贴板里已有 curl 内容，已自动填入，可直接点「解析并开始下载」。", "ok")
        except Exception:
            pass

    # -- 界面小工具 --
    def _load_icon(self):
        """设置窗口与任务栏图标；打包后从 _MEIPASS 读，开发时从脚本目录读"""
        base = getattr(sys, "_MEIPASS", HERE)
        ico = os.path.join(base, "uc_icon.ico")
        png = os.path.join(base, "uc_icon.png")
        if os.path.exists(ico):
            try:
                self.root.iconbitmap(default=ico)
                return True
            except Exception:
                pass
        if os.path.exists(png):
            try:
                self._icon_img = self.tk.PhotoImage(file=png)   # 必须持引用，否则被回收
                self.root.iconphoto(True, self._icon_img)
                return True
            except Exception:
                pass
        return False

    def msg(self, text, kind="info"):
        ts = time.strftime("%H:%M:%S")
        self.log.configure(state="normal")
        self.log.insert("end", f"[{ts}] {text}\n", kind)
        self.log.see("end")
        self.log.configure(state="disabled")

    def paste_clipboard(self):
        try:
            clip = self.root.clipboard_get()
        except Exception:
            self.msg("剪贴板里没有文本内容。", "warn")
            return
        if not clip.strip():
            self.msg("剪贴板是空的。", "warn")
            return
        self.text.delete("1.0", "end")
        self.text.insert("1.0", clip.strip())
        self.msg("已从剪贴板填入内容。", "ok")

    def set_busy(self, busy):
        self.btn_start.configure(state="disabled" if busy else "normal")
        self.btn_paste.configure(state="disabled" if busy else "normal")
        self.btn_stop.configure(state="normal" if busy else "disabled")
        self.conns.configure(state="disabled" if busy else "readonly")
        self.chunk.configure(state="disabled" if busy else "readonly")

    def open_folder(self):
        target = self.out_path or self.out_dir
        try:
            if target and os.path.exists(target) and os.path.isfile(target):
                subprocess.Popen(["explorer", "/select,", os.path.normpath(target)])
            else:
                os.startfile(os.path.normpath(os.path.dirname(target) if target else self.out_dir))
        except Exception as e:
            self.msg(f"打不开文件夹：{e}", "warn")

    def on_close(self):
        """关窗：先取消待触发的定时回调，再置停止标志、销毁窗口

        下载线程是 daemon 线程，主线程一结束它们会随进程一起被系统回收；
        这里不需要 join（也不会卡住关窗）。
        """
        self._closing = True
        self.stop_event.set()
        try:
            if self._poll_id:
                self.root.after_cancel(self._poll_id)
                self._poll_id = None
        except Exception:
            pass
        self.root.destroy()

    # -- 启动下载 --
    def start(self):
        raw = self.text.get("1.0", "end").strip()
        if not raw:
            self.msg("请先粘贴 Copy as cURL 的内容（只粘链接不行，UC 需要 Cookie 才能通过登录校验）。", "err")
            return
        try:
            parsed = parse_curl(raw)
        except Exception as e:
            self.msg(f"解析失败：{e}", "err")
            return
        if not parsed["cookie"]:
            self.msg("没在内容里找到 Cookie。请务必用 DevTools 的「Copy as cURL」整段复制，只粘链接无法通过 UC 登录校验。", "err")
            return

        name = filename_from_url(parsed["url"])
        self.out_path = os.path.join(self.out_dir, name)
        self.lbl_path.configure(text=f"保存到：{self.out_path}")
        self.msg(f"链接解析成功，目标文件：{name}", "ok")
        if os.path.exists(self.out_path + ".parts.json"):
            self.msg("检测到同名文件的未完成进度，将自动续传。", "warn")

        try:
            chunk_bytes = int(self.chunk.get().split()[0]) * 1024 * 1024
            conns = int(self.conns.get())
        except Exception:
            chunk_bytes, conns = 4 * 1024 * 1024, 64

        self.stop_event.clear()
        self.set_busy(True)
        self.pb.configure(value=0.0)
        dl = Downloader(parsed, self.out_path, conns, chunk_bytes, self.q, self.stop_event)
        self.dl = dl
        self.worker = threading.Thread(target=dl.run, daemon=True)
        self.worker.start()

    def stop(self):
        if self.worker and self.worker.is_alive():
            self.stop_event.set()
            self.msg("正在停止…（已下载的进度会保留，稍后可续传）", "warn")

    # -- 事件轮询 --
    def poll(self):
        if self._closing:
            return
        try:
            while True:
                ev = self.q.get_nowait()
                kind = ev[0]
                if kind == "log":
                    self.msg(ev[1], ev[2] if len(ev) > 2 else "info")
                elif kind == "progress":
                    self.update_progress(ev[1])
                elif kind == "done":
                    self.on_done(ev[1])
                elif kind == "failed":
                    self.msg(ev[1], "err")
                    self.msg("提示：重新在网页点下载 → Copy as cURL → 粘贴 → 点开始，即可继续断点续传。", "warn")
                    self.set_busy(False)
        except queue.Empty:
            pass
        if not self._closing:
            self._poll_id = self.root.after(400, self.poll)

    def update_progress(self, p):
        total = p["total"] or 1
        pct = p["done"] / total * 100.0
        self.pb.configure(value=pct)
        self.lbl_size.configure(
            text=f"{p['done']/1048576:8.1f} / {p['total']/1048576:.1f} MB"
                 f"      瞬时 {p['inst']:5.2f} MB/s     均速 {p['avg']:5.2f} MB/s")
        left = f"{p['left']:.0f}" if p["left"] else "--"
        self.lbl_chunk.configure(
            text=f"已完成 {p['chunks_done']}/{p['chunks_total']} 块          预计剩余 {left} 秒")
        self.root.title(f"{APP_TITLE} — {pct:.1f}% · {p['inst']:.1f} MB/s")

    def on_done(self, info):
        self.pb.configure(value=100.0)
        self.root.title(f"{APP_TITLE} — 已完成 100%")
        self.set_busy(False)
        self.msg(f"✅ 下载完成：{info['path']}", "ok")
        self.msg(f"耗时 {info['seconds']:.1f} 秒，速度 {info['total']/max(info['seconds'],0.1)/1048576:.2f} MB/s"
                 + ("（本次为续传）" if info.get("resumed") else ""), "ok")
        self.msg("点「打开所在文件夹」就能看到文件。", "info")


def main():
    import tkinter as tk
    args = sys.argv[1:]
    selftest = "--selftest" in args
    out_dir = None
    for i, a in enumerate(args):
        if a == "--out-dir" and i + 1 < len(args):
            out_dir = args[i + 1]
    try:
        root = tk.Tk()
        app = App(root, out_dir=out_dir, selftest=selftest)
    except Exception:
        # 打包成 exe 后没有控制台，启动崩溃要让人看见原因
        import traceback
        tb = traceback.format_exc()
        try:
            with open(os.path.join(HERE, "crash.log"), "a", encoding="utf-8") as f:
                f.write(time.strftime("%Y-%m-%d %H:%M:%S") + "\n" + tb + "\n")
        except Exception:
            pass
        try:
            from tkinter import messagebox
            messagebox.showerror(APP_TITLE, "程序启动失败：\n\n" + tb[-800:])
        except Exception:
            pass
        return 1
    if selftest:
        run_selftest(root, app, args)
    root.mainloop()
    return 0


def run_selftest(root, app, args):
    """自检：把 curl_test.txt 填进去，跑几秒，检查进度和标题是否更新，然后清理退出"""
    import tkinter as tk
    curl_file = os.path.join(HERE, "curl_test.txt")
    seconds = 10
    for i, a in enumerate(args):
        if a == "--seconds" and i + 1 < len(args):
            seconds = float(args[i + 1])
    if not os.path.exists(curl_file):
        print("SELFTEST: SKIP（没有 curl_test.txt）")
        root.after(200, root.destroy)
        return
    app.text.delete("1.0", "end")
    app.text.insert("1.0", open(curl_file, encoding="utf-8").read())
    app.conns.set("16")
    app.chunk.set("2 MB")

    result = {"title": "", "pct": 0.0, "speed": 0.0, "logs": []}

    def start_it():
        app.start()

    def check():
        result["title"] = root.title()
        result["icon"] = getattr(app, "icon_loaded", False)
        try:
            result["pct"] = float(app.pb["value"])
        except Exception:
            pass
        result["speed"] = app.dl.done_bytes if app.dl else 0
        result["logs"] = app.log.get("1.0", "end").strip().splitlines()
        app.stop_event.set()
        root.after(1500, finish)

    def finish():
        tmp = app.out_path
        for p in ([tmp, tmp + ".parts.json"] if tmp else []):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        ok = result["pct"] > 0 and "MB/s" in result["title"]
        lines = ["SELFTEST: " + ("PASS" if ok else "FAIL"),
                 "  进度条值: %.2f%%" % result["pct"],
                 "  窗口标题: " + result["title"],
                 "  已下载字节: %d" % result["speed"],
                 "  窗口图标: %s" % ("已加载" if result.get("icon") else "未加载"),
                 "  日志尾部:"]
        lines += ["    " + l for l in result["logs"][-6:]]
        text = "\n".join(lines)
        print(text)
        try:  # --noconsole 打包后没有 stdout，同时落盘一份结果
            with open(os.path.join(HERE, "selftest_result.txt"), "w", encoding="utf-8") as f:
                f.write(text + "\n")
        except Exception:
            pass
        root.destroy()

    root.after(500, start_it)
    root.after(int(seconds * 1000), check)


if __name__ == "__main__":
    sys.exit(main())
