#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UC/OSS 直链多线程分块下载器（带断点续传）

用法: python download.py [--conns 64] [--chunk 4] [--out 文件名]
读取同目录 url.txt (签名直链) 与 cookie.txt (Cookie 头)
"""
import argparse, json, os, ssl, sys, threading, time, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
URL = open(os.path.join(HERE, "url.txt"), encoding="utf-8").read().strip()
COOKIE = open(os.path.join(HERE, "cookie.txt"), encoding="utf-8").read().strip()

BASE_HEADERS = {
    "Cookie": COOKIE,
    "Referer": "https://fast.uc.cn/",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"),
}
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

stop = threading.Event()
lock = threading.Lock()
done_bytes = 0
errors = []


def head_size():
    """HEAD 取对象大小；失败则用 Range 探测"""
    req = urllib.request.Request(URL, headers=BASE_HEADERS, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
            if r.status == 200 and r.headers.get("Content-Length"):
                return int(r.headers["Content-Length"])
    except Exception:
        pass
    req = urllib.request.Request(URL, headers={**BASE_HEADERS, "Range": "bytes=0-0"})
    with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
        cr = r.headers.get("Content-Range", "")
        return int(cr.split("/")[-1])


def fetch_chunk(url, off, end, tries=6):
    """下载 [off, end] 并返回 bytes；None 表示放弃"""
    for attempt in range(tries):
        if stop.is_set():
            return None
        try:
            req = urllib.request.Request(url, headers={**BASE_HEADERS, "Range": f"bytes={off}-{end}"})
            with urllib.request.urlopen(req, timeout=90, context=CTX) as r:
                if r.status not in (200, 206):
                    raise IOError(f"HTTP {r.status}")
                buf = bytearray()
                want = end - off + 1
                while len(buf) < want:
                    b = r.read(min(262144, want - len(buf)))
                    if not b:
                        break
                    buf += b
                if len(buf) != want:
                    raise IOError(f"short read {len(buf)}/{want}")
                return bytes(buf)
        except urllib.error.HTTPError as e:
            body = e.read()[:200].decode("utf-8", "ignore").replace("\n", " ")
            if e.code == 403:
                with lock:
                    errors.append(f"403 被拒（链接过期或 Cookie 失效）: {body}")
                stop.set()
                return None
            with lock:
                errors.append(f"HTTP {e.code}: {body}")
        except Exception as e:
            with lock:
                errors.append(f"{type(e).__name__}: {str(e)[:80]}")
        time.sleep(min(2 ** attempt * 0.5, 5))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conns", type=int, default=64)
    ap.add_argument("--chunk", type=float, default=4.0, help="分块大小 MB")
    ap.add_argument("--out", default="doro916(1).mp4")
    args = ap.parse_args()

    out_path = args.out if os.path.isabs(args.out) else os.path.join(HERE, args.out)
    state_path = out_path + ".parts.json"
    CHUNK = int(args.chunk * 1024 * 1024)

    total = head_size()
    nchunks = (total + CHUNK - 1) // CHUNK

    done = set()
    if os.path.exists(state_path):
        try:
            st = json.load(open(state_path))
            if st.get("total") == total and st.get("chunk") == CHUNK:
                done = set(st["done"])
                print(f"[续传] 已完成 {len(done)}/{nchunks} 块")
        except Exception:
            pass

    mode = "r+b" if os.path.exists(out_path) and done else "wb"
    f = open(out_path, mode)
    if mode == "wb":
        f.truncate(total)
        done = set()
    f.truncate(total)

    pending = [i for i in range(nchunks) if i not in done]
    if not pending:
        print("已全部下载完成")
        return

    global done_bytes
    done_bytes = len(done) * CHUNK
    t0 = time.time()
    last = [time.time(), done_bytes]

    def save_state():
        tmp = state_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"total": total, "chunk": CHUNK, "done": sorted(done)}, fh)
        os.replace(tmp, state_path)

    def worker():
        global done_bytes
        while not stop.is_set():
            with lock:
                if not pending:
                    return
                idx = pending.pop(0)
            off = idx * CHUNK
            end = min(off + CHUNK, total) - 1
            data = fetch_chunk(URL, off, end)
            if data is None:
                with lock:
                    pending.insert(0, idx)
                return
            with lock:
                f.seek(off)
                f.write(data)
                done.add(idx)
                done_bytes += len(data)
                save_state()

    print(f"开始下载: {nchunks} 块 x {args.chunk}MB, {args.conns} 线程, 总计 {total/1048576:.1f} MB")
    ths = [threading.Thread(target=worker, daemon=True) for _ in range(args.conns)]
    for t in ths:
        t.start()

    try:
        while any(t.is_alive() for t in ths):
            time.sleep(0.5)
            now = time.time()
            if now - last[0] >= 1.0:
                inst = (done_bytes - last[1]) / (now - last[0]) / 1048576
                avg = (done_bytes) / (now - t0) / 1048576
                eta = (total - done_bytes) / max(inst * 1048576, 1)
                pct = done_bytes / total * 100
                bar = "#" * int(pct / 2.5)
                sys.stdout.write(
                    f"\r[{bar:<40}] {pct:5.1f}%  {done_bytes/1048576:7.1f}/{total/1048576:.1f}MB  "
                    f"瞬时 {inst:5.2f} MB/s  均速 {avg:5.2f} MB/s  ETA {eta:4.0f}s ")
                sys.stdout.flush()
                last[0], last[1] = now, done_bytes
    except KeyboardInterrupt:
        stop.set()
        print("\n[中断] 状态已保存，可重新运行续传")

    for t in ths:
        t.join(timeout=5)
    f.close()
    if stop.is_set() and errors:
        print("\n错误: " + errors[-1])
    print(f"\n完成 {len(done)}/{nchunks} 块, 耗时 {time.time()-t0:.1f}s, 文件 {os.path.getsize(out_path)} 字节")
    if len(done) == nchunks and not stop.is_set():
        os.remove(state_path) if os.path.exists(state_path) else None
        print("状态文件已清理")


if __name__ == "__main__":
    main()
