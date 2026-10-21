#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""任务列表的本地持久化。

只存任务元数据和状态；分块进度仍然由同名 .parts.json 负责。
提取码用 Windows DPAPI 加密，不裸存。
"""

import base64
import json
import os
import threading
import time

import uc_api

APP_DIR = uc_api.APP_DIR
TASKS_PATH = os.path.join(APP_DIR, "tasks.json")
VERSION = 1


def _encrypt_code(code):
    code = (code or "").strip()
    if not code:
        return ""
    try:
        return base64.b64encode(uc_api._dpapi(code.encode("utf-8"), protect=True)).decode("ascii")
    except Exception:
        return ""


def _decrypt_code(blob):
    if not blob:
        return ""
    try:
        return uc_api._dpapi(base64.b64decode(blob), protect=False).decode("utf-8")
    except Exception:
        return ""


class TaskStore:
    def __init__(self, path=TASKS_PATH):
        self.path = path
        self._lock = threading.RLock()
        self._records = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            rows = data.get("tasks") if isinstance(data, dict) else data
            if not isinstance(rows, list):
                raise ValueError("tasks 不是列表")
            for row in rows:
                if not isinstance(row, dict):
                    continue
                task_id = str(row.get("id") or "").strip()
                if not task_id:
                    continue
                self._records[task_id] = row
        except Exception:
            # 坏文件不阻塞启动：留个 .bad 备份，从空列表开始
            try:
                os.replace(self.path, self.path + ".bad")
            except Exception:
                pass
            self._records = {}

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            payload = {
                "version": VERSION,
                "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "tasks": list(self._records.values()),
            }
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
            return True
        except Exception:
            return False

    def records(self):
        with self._lock:
            return [dict(row) for row in self._records.values()]

    def upsert(self, record):
        task_id = str((record or {}).get("id") or "").strip()
        if not task_id:
            return False
        with self._lock:
            self._records[task_id] = dict(record)
            return self._save()

    def remove(self, task_id):
        with self._lock:
            if task_id in self._records:
                self._records.pop(task_id, None)
                return self._save()
        return True

    def encrypt_code(self, code):
        return _encrypt_code(code)

    def decrypt_code(self, blob):
        return _decrypt_code(blob)
