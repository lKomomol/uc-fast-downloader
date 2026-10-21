#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UC 多线程下载器（图形界面版）

用法:
    python uc_gui.py                # 打开界面
    python uc_gui.py --link "分享链接" [--passcode 1234] [--go]

使用方式：
    粘分享链接/二维码（有提取码就填上），点「解析并开始下载」。
    想快 5 倍可以点「登录」，用手机号+验证码登一次。
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

import uc_api
import qr_tools

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
                "     请重新点「开始」让程序再取一次直链；如果仍失败，点「清除」后重新登录。")
    if "SignatureDoesNotMatch" in body or code == 403:
        return ("403：直链已过期或被拒绝（直链有效期约 3 小时）。\n"
                "     请重新点「开始」让程序再取一次直链；已下载的进度会保留，可继续续传。")
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


def _short(text, n=76):
    text = str(text or "")
    return text if len(text) <= n else text[: n - 1] + "…"


class TaskQueue:
    """给每个任务套一层队列，下载线程只要往这里丢事件，界面按任务分发"""

    def __init__(self, app_q, task_id):
        self.app_q = app_q
        self.task_id = task_id

    def put(self, event):
        self.app_q.put(("task", self.task_id, event))


class DownloadTask:
    _next_id = 0

    def __init__(self, app, link, pwd_id, passcode):
        DownloadTask._next_id += 1
        self.id = f"task-{DownloadTask._next_id}"
        self.app = app
        self.link = link
        self.pwd_id = pwd_id
        self.code = passcode
        self.passcode = passcode
        self.title = f"分享 {pwd_id}"
        self.out_dir = app.current_out_dir()
        self.out_path = None
        self.conns = app.selected_conns()
        self.chunk = app.selected_chunk()
        self.parsed = None

        self.status = "resolving"       # resolving / downloading / pausing / paused / done / failed / needs_code
        self.pause_requested = False
        self.error = ""
        self.stop_event = threading.Event()
        self.cancel_resolve = threading.Event()
        self.worker = None

        self.done = 0
        self.total = 0
        self.inst = 0.0
        self.avg = 0.0
        self.left = 0.0
        self.chunks_done = 0
        self.chunks_total = 0
        self.started_at = time.time()

        self.q = TaskQueue(app.q, self.id)
        self.row = None
        self.name_lbl = None
        self.pb = None
        self.status_lbl = None
        self.btn_pause = None
        self.btn_del = None

    # -- 行界面 --
    def build_row(self, parent):
        row = self.app.tk.Frame(parent, bg="#ffffff", highlightbackground="#d9d9d9",
                                highlightthickness=1)
        row.pack(fill="x", padx=2, pady=2)
        row.columnconfigure(0, weight=1)
        self.row = row

        self.name_lbl = self.app.tk.Label(
            row, text=_short(self.title), anchor="w", bg="#ffffff",
            font=("Microsoft YaHei UI", 9, "bold"))
        self.name_lbl.grid(row=0, column=0, sticky="ew", padx=(8, 4), pady=(5, 0))

        self.btn_pause = self.app.tk.Button(
            row, text="⏸", width=3, command=lambda: self.app.toggle_pause(self),
            relief="groove", borderwidth=1, bg="#f5f5f5", activebackground="#e8e8e8",
            font=("Segoe UI Symbol", 11))
        self.btn_pause.grid(row=0, column=1, rowspan=3, sticky="ns", padx=(2, 0), pady=5)

        self.btn_del = self.app.tk.Button(
            row, text="✕", width=3, command=lambda: self.app.remove_task(self),
            relief="groove", borderwidth=1, bg="#f5f5f5", fg="#c5221f",
            activebackground="#ffe8e6", activeforeground="#c5221f",
            font=("Segoe UI Symbol", 10))
        self.btn_del.grid(row=0, column=2, rowspan=3, sticky="ns", padx=(2, 6), pady=5)

        self.pb = self.app.ttk.Progressbar(row, maximum=100.0, mode="determinate")
        self.pb.grid(row=1, column=0, sticky="ew", padx=(8, 4), pady=(4, 2))

        self.status_lbl = self.app.tk.Label(
            row, text="解析中…", anchor="w", bg="#ffffff", fg="#555555",
            font=("Consolas", 8))
        self.status_lbl.grid(row=2, column=0, sticky="ew", padx=(8, 4), pady=(0, 5))

        self.update_row()

    def update_row(self):
        if self.row is None:
            return
        total = self.total or 0
        pct = (self.done / total * 100.0) if total else (100.0 if self.status == "done" else 0.0)
        pct = max(0.0, min(100.0, pct))
        self.pb.configure(value=pct)
        self.name_lbl.configure(text=_short(self.title))

        if self.status == "resolving":
            text = "解析中…"
            color = "#555555"
        elif self.status == "downloading":
            if total:
                text = (f"{self.done / 1048576:8.1f} / {total / 1048576:.1f} MB"
                        f" · {self.inst:5.1f} MB/s · {pct:4.1f}%")
                if self.left:
                    text += f" · 剩 {self.left:.0f}s"
            else:
                text = "连接中…"
            color = "#333333"
        elif self.status == "pausing":
            text = "正在暂停…"
            color = "#b06000"
        elif self.status == "paused":
            text = f"已暂停 · {self.done / 1048576:.1f} / {total / 1048576:.1f} MB（{pct:.1f}%）"
            color = "#b06000"
        elif self.status == "done":
            text = f"完成 · {total / 1048576:.1f} MB"
            color = "#137333"
        elif self.status == "needs_code":
            text = "需要提取码"
            color = "#c5221f"
        else:
            text = "失败：" + _short(self.error or "未知错误", 90)
            color = "#c5221f"

        self.status_lbl.configure(text=text, fg=color)

        if self.status in ("resolving", "downloading"):
            self.btn_pause.configure(text="⏸", state="normal")
        elif self.status == "pausing":
            self.btn_pause.configure(text="…", state="disabled")
        elif self.status == "done":
            self.btn_pause.configure(text="✓", state="disabled")
        else:
            self.btn_pause.configure(text="▶", state="normal")

        self.app.refresh_title()

    # -- 解析 --
    def start_resolve(self):
        self.pause_requested = False
        self.cancel_resolve = threading.Event()
        self.stop_event = threading.Event()
        self.status = "resolving"
        self.error = ""
        self.update_row()
        self.worker = threading.Thread(target=self._resolve_worker, daemon=True)
        self.worker.start()

    def _resolve_worker(self):
        cancel = self.cancel_resolve
        try:
            parsed = uc_api.resolve_share(
                self.pwd_id, self.code, self.app.store.header(),
                on_step=lambda m: self.q.put(("log", m, "info")),
                cancel=cancel)
            self.q.put(("resolved", parsed))
        except uc_api.ShareCancelled:
            self.q.put(("resolve_cancelled", None))
        except Exception as e:
            self.q.put(("resolve_failed", (str(e), getattr(e, "code", None))))

    def on_resolved(self, parsed):
        if self.pause_requested or self.status != "resolving":
            self.mark_paused()
            return
        self.parsed = parsed
        self.title = parsed.get("filename") or self.title
        self.total = int(parsed.get("size") or 0)
        if not self.out_path:
            self.out_path = self.app.pick_out_path(self, self.title)
        self.status = "downloading"
        self.started_at = time.time()
        self.update_row()
        mode = parsed.get("mode")
        mode_text = "登录态（单连接约 500KB/s）" if mode == "login" else (
            "游客态（单连接约 100KB/s）" if mode == "guest" else "未知")
        self.app.msg(f"[{self.title}] 解析完成，{mode_text}；保存到 {self.out_path}", "ok")
        if os.path.exists(self.out_path + ".parts.json"):
            self.app.msg(f"[{self.title}] 发现上次进度，将自动续传。", "warn")
        self.start_download(parsed)

    def start_download(self, parsed):
        self.status = "downloading"
        self.pause_requested = False
        self.error = ""
        self.stop_event = threading.Event()
        self.update_row()
        self.worker = threading.Thread(target=self._download_worker, args=(parsed,), daemon=True)
        self.worker.start()

    def _download_worker(self, parsed):
        try:
            Downloader(parsed, self.out_path, self.conns, self.chunk, self.q, self.stop_event).run()
        except Exception as e:
            self.q.put(("failed", f"{type(e).__name__}: {e}"))

    # -- 暂停 / 继续 --
    def pause(self):
        if self.status not in ("resolving", "downloading"):
            return
        self.pause_requested = True
        self.cancel_resolve.set()
        self.stop_event.set()
        self.status = "pausing"
        self.update_row()
        self.app.msg(f"[{self.title}] 正在暂停，进度会保存…", "warn")

    def resume(self):
        if self.status not in ("paused", "failed", "needs_code"):
            return
        self.pause_requested = False
        self.error = ""
        self.start_resolve()

    def mark_paused(self):
        if self.status == "paused":
            return
        self.status = "paused"
        self.pause_requested = False
        self.update_row()
        self.app.msg(f"[{self.title}] 已暂停，进度已保存；点 ▶ 继续，不用重新粘链接。", "warn")

    def on_progress(self, p):
        self.done = p.get("done") or 0
        self.total = p.get("total") or self.total
        self.inst = p.get("inst") or 0.0
        self.avg = p.get("avg") or 0.0
        self.left = p.get("left") or 0.0
        self.chunks_done = p.get("chunks_done") or 0
        self.chunks_total = p.get("chunks_total") or 0
        self.update_row()

    def on_done(self, info):
        self.status = "done"
        self.done = info.get("total") or self.total
        self.total = self.done
        self.inst = 0.0
        self.left = 0.0
        self.update_row()
        secs = info.get("seconds") or 0.0
        speed = (self.total / secs / 1048576.0) if secs else 0.0
        self.app.msg(f"[{self.title}] 下载完成：{info.get('path')}（{self.total / 1048576:.1f} MB，{speed:.1f} MB/s）", "ok")

    def on_failed(self, msg):
        if self.pause_requested:
            self.mark_paused()
            return
        self.status = "failed"
        self.error = str(msg)
        self.update_row()
        self.app.msg(f"[{self.title}] 下载失败：{self.error}", "err")

    def on_resolve_failed(self, payload):
        msg, code = payload if isinstance(payload, (tuple, list)) else (str(payload), None)
        if self.pause_requested:
            self.mark_paused()
            return
        if code in uc_api.NEED_PASSCODE or code in uc_api.BAD_PASSCODE or "提取码" in str(msg):
            self.status = "needs_code"
            self.error = str(msg)
            self.update_row()
            self.app.ask_passcode(self, str(msg), code in uc_api.BAD_PASSCODE)
            return
        self.status = "failed"
        self.error = str(msg)
        self.update_row()
        self.app.msg(f"[{self.title}] 解析失败：{self.error}", "err")

    def handle_event(self, ev):
        kind = ev[0]
        if kind == "log":
            self.app.msg(f"[{self.title}] {ev[1]}", ev[2] if len(ev) > 2 else "info")
        elif kind == "progress":
            self.on_progress(ev[1])
        elif kind == "resolved":
            self.on_resolved(ev[1])
        elif kind == "resolve_cancelled":
            self.mark_paused()
        elif kind == "resolve_failed":
            self.on_resolve_failed(ev[1])
        elif kind == "done":
            self.on_done(ev[1])
        elif kind == "failed":
            self.on_failed(ev[1])


class App:
    def __init__(self, root, out_dir=None):
        import tkinter as tk
        from tkinter import filedialog, font as tkfont, ttk

        self.tk = tk
        self.ttk = ttk
        self.filedialog = filedialog
        self.root = root
        self.q = queue.Queue()
        self.tasks = {}
        self.store = uc_api.CookieStore()
        self.login_info = self.store.info()
        self.default_out_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        saved_dir = uc_api.load_settings().get("out_dir")
        if out_dir:
            self.out_dir = out_dir
        elif isinstance(saved_dir, str) and saved_dir.strip():
            self.out_dir = os.path.expanduser(saved_dir)
        else:
            self.out_dir = self.default_out_dir
        self.out_dir = os.path.normpath(self.out_dir)
        self._closing = False

        root.title(APP_TITLE)
        root.geometry("900x720")
        root.minsize(760, 600)
        try:
            tkfont.nametofont("TkDefaultFont").configure(family="Microsoft YaHei UI", size=10)
            tkfont.nametofont("TkTextFont").configure(family="Microsoft YaHei UI", size=10)
        except Exception:
            pass

        pad = {"padx": 10, "pady": 4}

        # ① 分享链接
        linkbar = ttk.Frame(root)
        linkbar.pack(fill="x", **pad)
        ttk.Label(linkbar, text="① 分享链接").pack(side="left")
        self.link = ttk.Entry(linkbar, font=("Consolas", 9))
        self.link.pack(side="left", fill="x", expand=True, padx=4)
        ttk.Label(linkbar, text="提取码(可空)").pack(side="left")
        self.passcode = ttk.Entry(linkbar, width=8)
        self.passcode.pack(side="left", padx=(2, 8))
        self.btn_login = tk.Button(linkbar, text="登录（手机号/验证码）", command=self.login,
                                   relief="groove", borderwidth=1, padx=8, pady=2,
                                   font=("Microsoft YaHei UI", 9))
        self.btn_login.pack(side="left")
        self.btn_logout = ttk.Button(linkbar, text="清除", width=6, command=self.logout)
        self.btn_logout.pack(side="left", padx=(4, 0))

        for w in (self.link, self.passcode):
            w.bind("<Return>", lambda e: self.start())

        self.lbl_login = ttk.Label(root, text=self._login_status_text(), foreground="#555555")
        self.lbl_login.pack(anchor="w", padx=10)
        self._update_login_ui()

        # 按钮行
        bar = ttk.Frame(root)
        bar.pack(fill="x", **pad)
        self.btn_start = ttk.Button(bar, text="解析并开始下载", command=self.start)
        self.btn_qr = ttk.Button(bar, text="粘贴二维码", command=self.paste_qr)
        self.btn_open = ttk.Button(bar, text="打开下载目录", command=self.open_folder)
        for w in (self.btn_start, self.btn_qr, self.btn_open):
            w.pack(side="left", padx=3)
        ttk.Label(bar, text="连接数").pack(side="left", padx=(14, 2))
        self.conns = ttk.Combobox(bar, width=4, values=("16", "32", "48", "64", "96", "128", "192"),
                                  state="readonly")
        self.conns.pack(side="left")
        ttk.Label(bar, text="分块").pack(side="left", padx=(10, 2))
        self.chunk = ttk.Combobox(bar, width=6, values=("1 MB", "2 MB", "4 MB", "8 MB"), state="readonly")
        self.chunk.pack(side="left")
        self.apply_defaults()

        # ② 保存位置
        savebar = ttk.Frame(root)
        savebar.pack(fill="x", padx=10, pady=(0, 2))
        ttk.Label(savebar, text="② 保存到").pack(side="left")
        self.out_dir_var = tk.StringVar(value=self.out_dir)
        self.dir_entry = ttk.Entry(savebar, textvariable=self.out_dir_var, font=("Consolas", 9))
        self.dir_entry.pack(side="left", fill="x", expand=True, padx=4)
        self.btn_browse = ttk.Button(savebar, text="浏览…", command=self.choose_dir)
        self.btn_browse.pack(side="left")
        self.btn_default = ttk.Button(savebar, text="默认", width=6, command=self.reset_dir)
        self.btn_default.pack(side="left", padx=(4, 0))

        # ③ 下载列表
        head = ttk.Frame(root)
        head.pack(fill="x", padx=10, pady=(6, 0))
        ttk.Label(head, text="③ 下载列表").pack(side="left")
        ttk.Label(head, text="可以连续添加多个任务；⏸ 暂停 / ▶ 继续 / ✕ 移除（文件保留）",
                  foreground="#777777", font=("Microsoft YaHei UI", 8)).pack(side="left", padx=8)

        list_frame = ttk.Frame(root)
        list_frame.pack(fill="x", padx=10, pady=(2, 4))
        self.task_canvas = tk.Canvas(list_frame, height=240, highlightthickness=0, bg="#fbfbfb")
        self.task_scroll = ttk.Scrollbar(list_frame, command=self.task_canvas.yview)
        self.task_inner = tk.Frame(self.task_canvas, bg="#fbfbfb")
        self._task_window = self.task_canvas.create_window((0, 0), window=self.task_inner, anchor="nw")
        self.task_inner.bind("<Configure>",
                             lambda e: self.task_canvas.configure(scrollregion=self.task_canvas.bbox("all")))
        self.task_canvas.bind("<Configure>",
                              lambda e: self.task_canvas.itemconfigure(self._task_window, width=e.width))
        self.task_canvas.configure(yscrollcommand=self.task_scroll.set)
        self.task_canvas.pack(side="left", fill="both", expand=True)
        self.task_scroll.pack(side="right", fill="y")
        self.task_canvas.bind("<Enter>", lambda e: self.task_canvas.bind_all("<MouseWheel>", self._on_wheel))
        self.task_canvas.bind("<Leave>", lambda e: self.task_canvas.unbind_all("<MouseWheel>"))

        self.empty_lbl = tk.Label(self.task_inner, text="还没有任务：粘分享链接或二维码，点「解析并开始下载」",
                                  bg="#fbfbfb", fg="#888888", font=("Microsoft YaHei UI", 9))
        self.empty_lbl.pack(pady=40)

        # ④ 日志
        ttk.Label(root, text="④ 日志：").pack(anchor="w", **pad)
        lb = ttk.Frame(root)
        lb.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log = tk.Text(lb, height=7, wrap="word", state="disabled", relief="solid",
                           borderwidth=1, font=("Microsoft YaHei UI", 9))
        lsb = ttk.Scrollbar(lb, command=self.log.yview)
        self.log.configure(yscrollcommand=lsb.set)
        self.log.pack(side="left", fill="both", expand=True)
        lsb.pack(side="right", fill="y")
        for k, c in LOG_COLORS.items():
            self.log.tag_configure(k, foreground=c)

        if self.login_info:
            self.msg("准备就绪：已保存登录态。粘分享链接/二维码即可，支持一次加多个任务。", "ok")
        else:
            self.msg("准备就绪：粘分享链接/二维码即可，支持一次加多个任务。想快 5 倍，点右上「登录」登一次。", "info")

        self.icon_loaded = self._load_icon()
        self._poll_id = self.root.after(400, self.poll)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # 剪贴板里已经是分享链接就自动填入
        try:
            clip = self.root.clipboard_get().strip()
            if uc_api.looks_like_share(clip):
                self.link.insert(0, clip)
                self.msg("检测到剪贴板里是分享链接，已自动填入。", "ok")
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

    def _update_login_ui(self):
        info = self.login_info or {}
        self.lbl_login.configure(text=self._login_status_text(),
                                 foreground="#137333" if info else "#555555")
        if info:
            self.btn_login.configure(text="✓ 已登录", bg="#d9f2dd", fg="#137333",
                                     activebackground="#c6ebce", activeforeground="#137333")
        else:
            self.btn_login.configure(text="登录（手机号/验证码）", bg="SystemButtonFace", fg="black",
                                     activebackground="SystemButtonFace", activeforeground="black")

    def choose_dir(self):
        try:
            d = self.filedialog.askdirectory(initialdir=self.out_dir_var.get() or self.default_out_dir,
                                             title="选择下载保存位置")
        except Exception as e:
            self.msg(f"打开目录选择框失败：{e}", "warn")
            return
        if d:
            d = os.path.normpath(d)
            self.out_dir_var.set(d)
            self.out_dir = d
            self._save_out_dir()
            self.msg(f"保存位置：{d}", "info")

    def reset_dir(self):
        self.out_dir_var.set(self.default_out_dir)
        self.out_dir = self.default_out_dir
        self._save_out_dir()
        self.msg("已恢复默认保存位置。", "info")

    def _save_out_dir(self):
        try:
            conf = uc_api.load_settings()
            conf["out_dir"] = self.out_dir_var.get().strip()
            uc_api.save_settings(conf)
        except Exception:
            pass

    def open_folder(self):
        try:
            target = self.out_dir_var.get().strip() or self.default_out_dir
            target = os.path.expandvars(os.path.expanduser(target))
            os.makedirs(target, exist_ok=True)
            os.startfile(os.path.normpath(target))
        except Exception as e:
            self.msg(f"打不开文件夹：{e}", "warn")

    def on_close(self):
        """关窗：置停止标志，让下载线程尽量保存进度"""
        self._closing = True
        for task in list(self.tasks.values()):
            try:
                task.cancel_resolve.set()
                task.stop_event.set()
            except Exception:
                pass
        try:
            if self._poll_id:
                self.root.after_cancel(self._poll_id)
                self._poll_id = None
        except Exception:
            pass
        self.root.destroy()

    # -- 登录态 --
    def _login_status_text(self):
        info = self.login_info or {}
        if info:
            mt = info.get("member_type") or "已登录"
            name = {"NORMAL": "普通用户", "VIP": "会员", "MINI_VIP": "迷你会员",
                    "SUPER_VIP": "超级会员", "Z_VIP": "Z会员"}.get(mt, mt)
            return f"登录态：{name}（{mt}）· 单连接约 500KB/s · 默认 64 连接 / 4MB 分块"
        return "登录态：游客 · 单连接约 100KB/s · 默认 128 连接 / 1MB（登录后快约 5 倍）"

    def apply_defaults(self):
        if self.login_info:
            self.conns.set("64")
            self.chunk.set("4 MB")
        else:
            self.conns.set("128")
            self.chunk.set("1 MB")

    def login(self):
        if self.login_info:
            self.msg("当前已经是登录状态；要换账号先点「清除」。", "warn")
            return
        self.btn_login.configure(state="disabled")
        self.msg("正在打开登录窗口…（手机号 + 验证码；滑块也在网页里拖）", "info")

        def run():
            try:
                cookies, info = uc_api.login_via_browser(
                    on_status=lambda m: self.q.put(("log", m, "info")))
                self.q.put(("login_ok", (cookies, info)))
            except Exception as e:
                self.q.put(("login_failed", str(e)))

        threading.Thread(target=run, daemon=True).start()

    def logout(self):
        self.store.clear()
        try:
            shutil.rmtree(uc_api.PROFILE_DIR, ignore_errors=True)
        except Exception:
            pass
        self.login_info = None
        self._update_login_ui()
        self.apply_defaults()
        self.msg("已清除登录态，回到游客模式。", "warn")

    # -- 任务 --
    def current_out_dir(self):
        d = self.out_dir_var.get().strip().strip('"') if hasattr(self, "out_dir_var") else self.out_dir
        if not d:
            d = self.default_out_dir
        d = os.path.expandvars(os.path.expanduser(d))
        os.makedirs(d, exist_ok=True)
        self.out_dir = os.path.normpath(d)
        return self.out_dir

    def selected_conns(self):
        try:
            return max(1, int(self.conns.get()))
        except Exception:
            return 64 if self.login_info else 128

    def selected_chunk(self):
        try:
            return max(1, int(self.chunk.get().split()[0])) * 1024 * 1024
        except Exception:
            return (4 if self.login_info else 1) * 1024 * 1024

    def pick_out_path(self, task, filename):
        if task.out_path:
            return task.out_path
        filename = sanitize(filename)
        base, ext = os.path.splitext(filename)
        candidate = os.path.join(task.out_dir, filename)
        used = {os.path.normcase(t.out_path) for t in self.tasks.values()
                if t is not task and t.out_path}
        if os.path.exists(candidate + ".parts.json") and os.path.normcase(candidate) not in used:
            return candidate
        n = 1
        while os.path.exists(candidate) or os.path.normcase(candidate) in used:
            n += 1
            candidate = os.path.join(task.out_dir, f"{base} ({n}){ext}")
        return candidate

    def add_task(self, task):
        task.build_row(self.task_inner)
        self.tasks[task.id] = task
        try:
            self.empty_lbl.pack_forget()
        except Exception:
            pass
        self._refresh_scroll()
        self.msg(f"已加入下载列表：{task.pwd_id}，开始解析…", "info")

    def remove_task(self, task):
        if task.id not in self.tasks:
            return
        try:
            task.cancel_resolve.set()
            task.stop_event.set()
        except Exception:
            pass
        self.tasks.pop(task.id, None)
        try:
            if task.row is not None:
                task.row.destroy()
        except Exception:
            pass
        if not self.tasks:
            try:
                self.empty_lbl.pack(pady=40)
            except Exception:
                pass
        self._refresh_scroll()
        self.msg(f"已移除任务：{task.title}（已下载的文件保留）", "warn")

    def toggle_pause(self, task):
        if task.status in ("resolving", "downloading"):
            task.pause()
        elif task.status in ("paused", "failed", "needs_code"):
            task.resume()

    def _refresh_scroll(self):
        try:
            self.task_inner.update_idletasks()
            self.task_canvas.configure(scrollregion=self.task_canvas.bbox("all"))
        except Exception:
            pass

    def _on_wheel(self, event):
        try:
            self.task_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass

    def refresh_title(self):
        if self._closing:
            return
        active = [t for t in self.tasks.values()
                  if t.status in ("resolving", "downloading", "pausing")]
        if active:
            speed = sum(t.inst for t in active if t.status == "downloading")
            self.root.title(f"{APP_TITLE} — {len(active)} 个任务进行中 · {speed:.1f} MB/s")
        else:
            self.root.title(APP_TITLE)

    # -- 开始任务 / 二维码 --
    def start(self):
        link = self.link.get().strip()
        if not link:
            self.msg("先把分享链接粘进「① 分享链接」（有提取码就填上）。", "err")
            self.link.focus_set()
            return
        try:
            pwd_id, pwd = uc_api.parse_share(link)
        except Exception as e:
            self.msg(f"链接解析失败：{e}", "err")
            self.link.focus_set()
            return
        code = self.passcode.get().strip() or pwd
        try:
            self.current_out_dir()
        except Exception as e:
            self.msg(f"保存目录不可用：{self.out_dir_var.get()}（{e}）", "err")
            return
        self._save_out_dir()

        task = DownloadTask(self, link, pwd_id, code)
        self.add_task(task)
        self.link.delete(0, "end")
        self.passcode.delete(0, "end")
        task.start_resolve()

    def paste_qr(self):
        """从剪贴板读二维码，识别后填入链接/提取码并开始"""
        if self._closing:
            return
        self.msg("正在从剪贴板读取二维码…", "info")
        threading.Thread(target=self._qr_worker, daemon=True).start()

    def _qr_worker(self):
        try:
            info = qr_tools.read_clipboard_share()
            self.q.put(("qr_ok", info))
        except qr_tools.QRCodeError as e:
            self.q.put(("qr_failed", str(e)))
        except Exception as e:
            self.q.put(("qr_failed", f"识别失败：{type(e).__name__}: {e}"))

    def ask_passcode(self, task, msg, bad=False):
        """只有服务端明确说需要提取码时才弹这个框；输入后自动重试"""
        if task.id not in self.tasks:
            return
        from tkinter import simpledialog
        prompt = f"分享「{task.title}」需要提取码："
        if bad:
            prompt = f"提取码不对，请重新输入。\n\n{msg}"
        while True:
            code = simpledialog.askstring("需要提取码", prompt, parent=self.root)
            if code is None:
                task.status = "failed"
                task.error = "未输入提取码"
                task.update_row()
                self.msg(f"[{task.title}] 未输入提取码；任务留在列表里，点 ▶ 可以重试。", "warn")
                return
            code = code.strip()
            if code:
                break
        if task.id not in self.tasks:
            return
        task.code = code
        task.passcode = code
        self.msg(f"[{task.title}] 已输入提取码，重新解析…", "info")
        task.start_resolve()

    # -- 事件循环 --
    def poll(self):
        if self._closing:
            return
        try:
            while True:
                ev = self.q.get_nowait()
                kind = ev[0]
                if kind == "task":
                    _, task_id, inner = ev
                    task = self.tasks.get(task_id)
                    if task is not None:
                        task.handle_event(inner)
                elif kind == "log":
                    self.msg(ev[1], ev[2] if len(ev) > 2 else "info")
                elif kind == "qr_ok":
                    info = ev[1]
                    self.link.delete(0, "end")
                    self.link.insert(0, info["url"])
                    self.passcode.delete(0, "end")
                    if info.get("passcode"):
                        self.passcode.insert(0, info["passcode"])
                        self.msg("二维码识别成功，已填入链接 + 提取码，开始下载。", "ok")
                    else:
                        self.msg("二维码里只有链接，先直接开始；如果 UC 提示需要提取码，会弹窗让你输入。", "warn")
                    self.start()
                elif kind == "qr_failed":
                    self.msg(f"二维码：{ev[1]}", "err")
                elif kind == "login_ok":
                    cookies, info = ev[1]
                    try:
                        self.store.save(cookies, info)
                    except Exception as e:
                        self.msg(f"登录成功，但保存登录态失败：{e}", "warn")
                    self.login_info = info or {"member_type": "已登录"}
                    self._update_login_ui()
                    self.apply_defaults()
                    self.msg("登录成功，新任务会切到高速默认值（64 连接 / 4MB）。", "ok")
                    self.btn_login.configure(state="normal")
                elif kind == "login_failed":
                    self.msg(f"登录没成功：{ev[1]}", "err")
                    self.btn_login.configure(state="normal")
        except queue.Empty:
            pass
        if not self._closing:
            self._poll_id = self.root.after(400, self.poll)


def main():
    import tkinter as tk

    args = sys.argv[1:]
    import json

    # 诊断用：不打开界面，识别剪贴板/指定图片里的二维码，结果写到 %LOCALAPPDATA%\UC下载器\qr_test.txt
    if "--qr-test" in args:
        i = args.index("--qr-test")
        image_path = args[i + 1] if i + 1 < len(args) and not args[i + 1].startswith("--") else ""
        out = {"time": time.strftime("%Y-%m-%d %H:%M:%S")}
        try:
            if image_path:
                from PIL import Image
                with Image.open(image_path) as im:
                    text = qr_tools.decode_qr(im.convert("RGB"))
                info = qr_tools.parse_share_text(text)
            else:
                info = qr_tools.read_clipboard_share()
            out.update(ok=True, **info)
        except Exception as e:
            out.update(ok=False, error=f"{type(e).__name__}: {e}")
        os.makedirs(uc_api.APP_DIR, exist_ok=True)
        with open(os.path.join(uc_api.APP_DIR, "qr_test.txt"), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)
        return 0

    # 诊断用：不打开界面，直接跑一次分享解析，结果写到 %LOCALAPPDATA%\UC下载器\resolve_test.txt
    if "--resolve-test" in args:
        import traceback
        i = args.index("--resolve-test")
        link = args[i + 1] if i + 1 < len(args) else ""
        extra_code = args[i + 2] if i + 2 < len(args) and not args[i + 2].startswith("--") else ""
        out = {"time": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": []}
        try:
            pwd, code = uc_api.parse_share(link)
            code = extra_code or code
            out["pwd_id"] = pwd
            r = uc_api.resolve_share(pwd, code, uc_api.CookieStore().header(),
                                     on_step=lambda m: out["steps"].append(m))
            out.update(ok=True, mode=r["mode"], name=r["filename"], size=r["size"])
        except Exception as e:
            out.update(ok=False, error=f"{type(e).__name__}: {e}", tb=traceback.format_exc()[-2000:])
        os.makedirs(uc_api.APP_DIR, exist_ok=True)
        with open(os.path.join(uc_api.APP_DIR, "resolve_test.txt"), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)
        return 0

    out_dir = None
    link_arg = None
    code_arg = None
    auto_go = "--go" in args
    for i, a in enumerate(args):
        if a == "--out-dir" and i + 1 < len(args):
            out_dir = args[i + 1]
        elif a == "--link" and i + 1 < len(args):
            link_arg = args[i + 1]
        elif a == "--passcode" and i + 1 < len(args):
            code_arg = args[i + 1]
    try:
        root = tk.Tk()
        app = App(root, out_dir=out_dir)
        if link_arg:
            app.link.insert(0, link_arg)
        if code_arg:
            app.passcode.insert(0, code_arg)
        if auto_go and link_arg:
            root.after(600, app.start)
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
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
