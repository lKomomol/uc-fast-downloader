#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""剪贴板二维码读取 + UC 分享内容提取。

只做三件事：
1. 从 Windows 剪贴板拿图片（截图、复制的图片、复制的图片文件都可以）
2. 识别二维码文字（zxing-cpp）
3. 从文字里提取 UC 分享链接和提取码

依赖：Pillow、zxing-cpp
"""

import os
import re

import uc_api

_IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".webp", ".gif", ".tif", ".tiff")


class QRCodeError(Exception):
    """二维码读取/识别失败，message 直接给用户看"""


def clipboard_has_image():
    """只看剪贴板里有没有图片，不解码；用于 Ctrl+V 判断"""
    try:
        from PIL import Image, ImageGrab
        data = ImageGrab.grabclipboard()
        if isinstance(data, Image.Image):
            return True
        if isinstance(data, (list, tuple)):
            return any(isinstance(item, str) and item.lower().endswith(_IMAGE_EXTS)
                       and os.path.isfile(item) for item in data)
    except Exception:
        pass
    return False


def _clipboard_image():
    try:
        from PIL import Image, ImageGrab
    except Exception as e:  # pragma: no cover - 只在缺依赖时走到
        raise QRCodeError("缺少 Pillow，无法读取剪贴板图片。请先安装：pip install pillow zxing-cpp") from e

    try:
        data = ImageGrab.grabclipboard()
    except Exception as e:
        raise QRCodeError(f"读取剪贴板失败：{e}") from e

    if isinstance(data, Image.Image):
        return data.convert("RGB")

    # 在资源管理器里复制图片文件时，Windows 给的是文件路径列表
    if isinstance(data, (list, tuple)):
        last_error = None
        for item in data:
            if not isinstance(item, str) or not os.path.isfile(item):
                continue
            if not item.lower().endswith(_IMAGE_EXTS):
                continue
            try:
                with Image.open(item) as im:
                    return im.convert("RGB")
            except Exception as e:
                last_error = e
        if last_error:
            raise QRCodeError(f"剪贴板里的图片打不开：{last_error}") from last_error
        raise QRCodeError("剪贴板里是文件，但不是能识别的图片。请复制二维码图片本身，而不是文件路径。")

    raise QRCodeError("剪贴板里没有图片。先把二维码截图/复制成图片，再点「粘贴二维码」。")


def _read_with_zxing(image):
    try:
        import zxingcpp
    except Exception:
        return ""
    results = zxingcpp.read_barcodes(image)
    texts = []
    for result in results:
        text = (getattr(result, "text", "") or "").strip()
        if not text:
            continue
        fmt = str(getattr(result, "format", ""))
        if "QR" in fmt or not fmt:
            texts.append(text)
    if not texts:
        return ""
    # 一张图里可能同时有多个码，优先选带 UC 域名的那个
    for text in texts:
        if "uc.cn" in text.lower():
            return text
    return texts[0]


def decode_qr(image):
    """从 PIL.Image 里识别二维码文字；识别不到抛 QRCodeError"""
    try:
        from PIL import Image, ImageOps
    except Exception as e:
        raise QRCodeError("缺少 Pillow，无法处理二维码图片。") from e
    try:
        import zxingcpp  # noqa: F401
    except Exception as e:
        raise QRCodeError("缺少 zxing-cpp，无法识别二维码。请安装：pip install zxing-cpp") from e

    gray = image.convert("L")
    attempts = [image, gray]

    # 截图里的二维码经常很小，放大后再试；反色也试一下（深色背景的二维码）
    w, h = gray.size
    if min(w, h) < 700:
        scale = max(2, int(700 / max(1, min(w, h))) + 1)
        big = gray.resize((w * scale, h * scale), Image.LANCZOS)
        attempts.extend([big, ImageOps.autocontrast(big), ImageOps.invert(big)])

    last_error = None
    for candidate in attempts:
        for reader in (_read_with_zxing,):
            try:
                text = reader(candidate)
            except Exception as e:
                last_error = e
                continue
            if text:
                return text

    detail = f"（{last_error}）" if last_error else ""
    raise QRCodeError("没识别出二维码。试试把二维码放大、截清晰一点，或者换一张图再试。" + detail)


def parse_share_text(text):
    """把二维码文字解析成 {url, pwd_id, passcode, raw}"""
    raw = (text or "").strip()
    if not raw:
        raise QRCodeError("二维码是空的。")

    url = uc_api.extract_share_url(raw)
    if not url:
        # 少数二维码只放了裸分享 ID
        if re.fullmatch(r"[0-9A-Za-z]{8,}", raw):
            url = f"https://drive.uc.cn/s/{raw}"
        else:
            shown = raw.replace("\n", " ")[:80]
            raise QRCodeError(f"识别到二维码，但里面没有 UC 分享链接。内容：{shown}")

    try:
        pwd_id, code = uc_api.parse_share(raw)
    except Exception as e:
        raise QRCodeError(f"二维码里有链接，但解析失败：{e}") from e

    return {"url": url, "pwd_id": pwd_id, "passcode": code, "raw": raw}


def read_clipboard_share():
    """完整流程：剪贴板图片 -> 二维码文字 -> UC 链接/提取码"""
    image = _clipboard_image()
    text = decode_qr(image)
    return parse_share_text(text)
