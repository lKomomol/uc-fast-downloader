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


class App:
    def __init__(self, root, out_dir=None):
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
        self._resolving = False
        self._resolve_gen = 0
        self._resolve_timer = None
        self._resolve_cancel = threading.Event()
        self.default_out_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        saved_dir = uc_api.load_settings().get("out_dir")
        if out_dir:
            self.out_dir = out_dir
        elif isinstance(saved_dir, str) and saved_dir.strip():
            self.out_dir = os.path.expanduser(saved_dir)
        else:
            self.out_dir = self.default_out_dir
        self.out_dir = os.path.normpath(self.out_dir)
        self.out_path = None
        self.store = uc_api.CookieStore()
        self.login_info = self.store.info()

        root.title(APP_TITLE)
        root.geometry("780x600")
        root.minsize(700, 540)
        try:
            tkfont.nametofont("TkDefaultFont").configure(family="Microsoft YaHei UI", size=10)
            tkfont.nametofont("TkTextFont").configure(family="Microsoft YaHei UI", size=10)
        except Exception:
            pass

        pad = {"padx": 10, "pady": 4}

        # ① 分享链接（推荐）
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
        self.btn_qr = ttk.Button(bar, text="粘贴二维码", command=self.paste_qr)
        self.btn_start = ttk.Button(bar, text="解析并开始下载", command=self.start)
        self.btn_stop = ttk.Button(bar, text="停止", command=self.stop, state="disabled")
        self.btn_open = ttk.Button(bar, text="打开所在文件夹", command=self.open_folder)
        for w in (self.btn_qr, self.btn_start, self.btn_stop, self.btn_open):
            w.pack(side="left", padx=3)
        ttk.Label(bar, text="连接数").pack(side="left", padx=(14, 2))
        self.conns = ttk.Combobox(bar, width=4, values=("16", "32", "48", "64", "96", "128", "192"),
                                  state="readonly")
        self.conns.pack(side="left")
        ttk.Label(bar, text="分块").pack(side="left", padx=(10, 2))
        self.chunk = ttk.Combobox(bar, width=6, values=("1 MB", "2 MB", "4 MB", "8 MB"), state="readonly")
        self.chunk.pack(side="left")
        self.apply_defaults()

        # ③ 保存位置
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

        # ④ 进度条
        ttk.Label(root, text="③ 进度：").pack(anchor="w", **pad)
        self.pb = ttk.Progressbar(root, maximum=100.0, mode="determinate")
        self.pb.pack(fill="x", padx=10)
        self.lbl_size = ttk.Label(root, text="0.0 / 0.0 MB          瞬时 0.00 MB/s     均速 0.00 MB/s",
                                  font=("Consolas", 10))
        self.lbl_size.pack(anchor="w", **pad)
        self.lbl_chunk = ttk.Label(root, text="已完成 0/0 块          预计剩余 -- 秒",
                                   font=("Consolas", 10))
        self.lbl_chunk.pack(anchor="w", padx=10)
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

        if self.login_info:
            self.msg("准备就绪：已保存登录态，直接粘分享链接就能全速下载。", "ok")
        else:
            self.msg("准备就绪：粘分享链接 + 提取码即可。想快 5 倍，点右上「登录」用手机号验证码登一次。", "info")
        self.icon_loaded = self._load_icon()
        self._closing = False
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

    def set_busy(self, busy):
        self.btn_qr.configure(state="disabled" if busy else "normal")
        self.btn_start.configure(state="disabled" if busy else "normal")
        self.btn_stop.configure(state="normal" if busy else "disabled")
        self.conns.configure(state="disabled" if busy else "readonly")
        self.chunk.configure(state="disabled" if busy else "readonly")
        for w in (self.link, self.passcode, self.dir_entry):
            w.configure(state="disabled" if busy else "normal")
        for w in (self.btn_login, self.btn_logout, self.btn_browse, self.btn_default):
            w.configure(state="disabled" if busy else "normal")

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
            if self.out_path and os.path.isfile(self.out_path):
                subprocess.Popen(["explorer", "/select,", os.path.normpath(self.out_path)])
            else:
                target = self.out_dir_var.get().strip() or self.default_out_dir
                os.makedirs(target, exist_ok=True)
                os.startfile(os.path.normpath(target))
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
            import shutil
            shutil.rmtree(uc_api.PROFILE_DIR, ignore_errors=True)
        except Exception:
            pass
        self.login_info = None
        self._update_login_ui()
        self.apply_defaults()
        self.msg("已清除登录态，回到游客模式。", "warn")

    # -- 二维码 --
    def paste_qr(self):
        """从剪贴板读二维码，识别后填入链接/提取码并自动开始"""
        if getattr(self, "_closing", False):
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

    # -- 启动下载 --
    def start(self):
        link = self.link.get().strip()
        if not link:
            self.msg("先把分享链接粘进「① 分享链接」（有提取码就填上）。", "err")
            return
        try:
            pwd_id, pwd = uc_api.parse_share(link)
        except Exception as e:
            self.msg(f"链接解析失败：{e}", "err")
            return
        code = self.passcode.get().strip() or pwd
        if not self.login_info:
            self.msg("游客模式：单连接约 100KB/s；要点「登录」才能快 5 倍。", "warn")
        self.stop_event.clear()
        self._resolving = True
        self._resolve_gen += 1
        gen = self._resolve_gen
        self._resolve_cancel = threading.Event()
        self.msg(f"正在解析分享 {pwd_id} …", "info")
        self.set_busy(True)
        self.pb.configure(value=0.0)
        try:
            threading.Thread(target=self._resolve_worker,
                             args=(pwd_id, code, gen, self._resolve_cancel), daemon=True).start()
        except Exception as e:
            self._resolving = False
            self._resolve_gen += 1
            self._cancel_resolve_timer()
            self.set_busy(False)
            self.msg(f"启动解析线程失败：{type(e).__name__}: {e}", "err")
            return
        if self._resolve_timer:
            try:
                self.root.after_cancel(self._resolve_timer)
            except Exception:
                pass
        self._resolve_timer = self.root.after(45000, self._resolve_timeout)

    def _resolve_worker(self, pwd_id, code, gen, cancel_event):
        try:
            parsed = uc_api.resolve_share(
                pwd_id, code, self.store.header(),
                on_step=lambda m: self.q.put(("log", m, "info")),
                cancel=cancel_event)
            self.q.put(("resolved", (gen, parsed)))
        except uc_api.ShareCancelled:
            self.q.put(("resolve_cancelled", (gen,)))
        except Exception as e:
            import traceback
            try:
                os.makedirs(uc_api.APP_DIR, exist_ok=True)
                with open(os.path.join(uc_api.APP_DIR, "error.log"), "a", encoding="utf-8") as f:
                    f.write(time.strftime("%Y-%m-%d %H:%M:%S") + "\n" + traceback.format_exc() + "\n")
            except Exception:
                pass
            self.q.put(("resolve_failed", (gen, str(e))))

    def _cancel_resolve_timer(self):
        if self._resolve_timer:
            try:
                self.root.after_cancel(self._resolve_timer)
            except Exception:
                pass
            self._resolve_timer = None

    def _resolve_timeout(self):
        self._resolve_timer = None
        if self._resolving:
            self.msg("解析超过 45 秒，网络可能卡住了；可以点「停止」取消，或检查网络后重试。", "warn")

    def _begin_download(self, parsed):
        d = self.out_dir_var.get().strip().strip('"')
        if not d:
            d = self.default_out_dir
        d = os.path.expandvars(os.path.expanduser(d))
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            self.msg(f"保存目录不可用：{d}（{e}）", "err")
            self.set_busy(False)
            return
        self.out_dir = os.path.normpath(d)
        self.out_dir_var.set(self.out_dir)
        self._save_out_dir()
        name = parsed.get("filename") or filename_from_url(parsed["url"])
        self.out_path = os.path.join(self.out_dir, name)
        size = parsed.get("size") or 0
        extra = f"（{size/1048576:.1f} MB）" if size else ""
        self.msg(f"目标文件：{name}{extra}", "ok")
        more = [f for f in (parsed.get("files") or []) if not f.get("dir")]
        if len(more) > 1:
            self.msg(f"分享里有 {len(more)} 个文件，这次先下第一个：{name}", "warn")
        if parsed.get("mode") == "login":
            self.msg("直链已确认是登录态：单连接约 500KB/s。", "ok")
        elif parsed.get("mode") == "guest":
            self.msg("直链是游客态：单连接约 100KB/s；登录后约 5 倍。", "warn")
        if os.path.exists(self.out_path + ".parts.json"):
            self.msg("检测到同名文件的未完成进度，将自动续传。", "warn")

        try:
            chunk_bytes = int(self.chunk.get().split()[0]) * 1024 * 1024
            conns = int(self.conns.get())
        except Exception:
            chunk_bytes = (4 if self.login_info else 1) * 1024 * 1024
            conns = 64 if self.login_info else 128

        self.stop_event.clear()
        self.set_busy(True)
        self.pb.configure(value=0.0)
        dl = Downloader(parsed, self.out_path, conns, chunk_bytes, self.q, self.stop_event)
        self.dl = dl
        self.worker = threading.Thread(target=dl.run, daemon=True)
        try:
            self.worker.start()
        except Exception as e:
            self.msg(f"启动下载线程失败：{type(e).__name__}: {e}", "err")
            self.set_busy(False)

    def stop(self):
        self.stop_event.set()
        if self._resolving:
            # 解析线程可能卡在网络调用里，先让界面立刻恢复
            self._resolve_cancel.set()
            self._resolve_gen += 1
            self._resolving = False
            self._cancel_resolve_timer()
            self.set_busy(False)
            self.msg("已取消解析。", "warn")
        elif self.worker and self.worker.is_alive():
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
                elif kind == "resolved":
                    gen, parsed = ev[1]
                    if gen != self._resolve_gen:
                        continue
                    self._resolving = False
                    self._cancel_resolve_timer()
                    self._begin_download(parsed)
                elif kind == "resolve_failed":
                    gen, text = ev[1]
                    if gen != self._resolve_gen:
                        continue
                    self._resolving = False
                    self._cancel_resolve_timer()
                    self.msg(text, "err")
                    if "提取码" in text:
                        self.passcode.focus_set()
                    self.set_busy(False)
                elif kind == "resolve_cancelled":
                    gen = ev[1][0]
                    if gen != self._resolve_gen:
                        continue
                    self._resolving = False
                    self._cancel_resolve_timer()
                    self.msg("已取消解析。", "warn")
                    self.set_busy(False)
                elif kind == "qr_ok":
                    info = ev[1]
                    self.link.delete(0, "end")
                    self.link.insert(0, info["url"])
                    self.passcode.delete(0, "end")
                    if info.get("passcode"):
                        self.passcode.insert(0, info["passcode"])
                    if self._resolving or (self.worker is not None and self.worker.is_alive()):
                        self.msg("二维码已识别并填入；当前有任务在跑，等它结束再点「解析并开始下载」。", "warn")
                        continue
                    if not info.get("passcode"):
                        # 只有链接：提取码可能是对方单独发的，留给用户补
                        self.passcode.focus_set()
                        self.msg("二维码里只有链接；提取码请单独输入。不需要提取码就直接按回车或点「解析并开始下载」。", "warn")
                        continue
                    self.msg("二维码识别成功，已填入链接 + 提取码，开始下载。", "ok")
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
                    self.msg("登录成功，已切到高速默认值（64 连接 / 4MB）。", "ok")
                    self.btn_login.configure(state="normal")
                elif kind == "login_failed":
                    self.msg(f"登录没成功：{ev[1]}", "err")
                    self.btn_login.configure(state="normal")
                elif kind == "failed":
                    self.msg(ev[1], "err")
                    self.msg("提示：分享链接直接再点一次「开始」即可续传。", "warn")
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
        import json
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
