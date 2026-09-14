#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
srt2json.py — Công cụ SRT/VTT → JSON · Tách từng câu · Dịch Nhật → Việt
=======================================================================

Pipeline:  SRT / VTT → Đọc timestamp → Tách từng câu → JA + VI → JSON

JSON đầu ra (đúng định dạng yêu cầu):

    {
      "cues": [
        {
          "s": 47.29,
          "e": 49.79,
          "ja": "（彼氏）はい",
          "vi": "Vâng."
        }
      ]
    }

CÁCH DÙNG
---------
1) Giao diện web (khuyên dùng — chỉ cần chạy, không cần cài gì thêm):

       python srt2json.py

   → trình duyệt tự mở http://127.0.0.1:8321
   → kéo-thả file .srt / .vtt (hoặc JSON cũ còn câu thiếu để ĐIỀN NỐT)
   → xem tiến độ → Tải JSON.

2) Dòng lệnh (CLI):

       python srt2json.py video.srt -o video.json   # dịch và ghi ra file
       python srt2json.py video.srt                  # in JSON ra màn hình
       python srt2json.py video.json                 # ĐIỀN NỐT câu thiếu trong JSON cũ
       python srt2json.py video.srt --workers 8      # tăng số luồng cho nhanh

2 ENGINE DỊCH
-------------
* google (mặc định — online): nhanh nhất, chất lượng tốt nhất, miễn phí,
  không cần key. Nhược điểm: gửi request qua mạng; chạy quá nhiều có thể
  bị Google chặn tạm (tool tự chờ nguội + 4 vòng thử lại + cache giúp
  đỡ phải dịch lại).
* local (offline — NLLB-200): KHÔNG cần internet, không bao giờ bị chặn,
  dịch thẳng Nhật→Việt ngay trên máy. Chất lượng khá (hơi kém Google).
  Cần cài 1 lần:

      pip install ctranslate2 sentencepiece huggingface_hub

  và lần chạy đầu sẽ tự tải model (~650MB) vào thư mục models/ cạnh tool.
  Sau đó chạy offline mãi. Cần ~1.5GB RAM trống. Dùng:

      python srt2json.py video.srt --engine local
      (trên web UI: Tuỳ chọn nâng cao → Engine dịch → Offline)

CACHE DỊCH (mới)
----------------
Mọi câu đã dịch được ghi vào srt2json_cache.json cạnh tool:
chạy lại cùng file (hay bất kỳ file có câu trùng) gần như TỨC THÌ,
chế độ điền nốt cũng rẻ hơn nhiều. Tắt bằng --no-cache.

TUỲ CHỌN CLI
------------
    -o, --output FILE    file JSON đầu ra (mặc định: in ra màn hình;
                         chế độ điền nốt: ghi đè lên file vào nếu bỏ trống)
    --engine {google,local}  engine dịch (mặc định: google)
    --no-cache           không dùng/ghi cache dịch
    --no-translate       không dịch, trường "vi" để rỗng
    --split              tách câu dài thành nhiều câu theo dấu 。！？
    --keep-tags          giữ thẻ định dạng <i>, <font>... (mặc định: lược bỏ)
    --workers N          số luồng dịch song song, engine google (mặc định: 5)
    --src LANG           ngôn ngữ nguồn  (mặc định: ja)
    --tgt LANG           ngôn ngữ đích   (mặc định: vi)
    --host HOST --port PORT   cho chế độ web (mặc định 127.0.0.1:8321)
    --no-browser         không tự mở trình duyệt

GHI CHÚ
-------
- KHÔNG cần cài thư viện ngoài cho engine google — chỉ thư viện chuẩn Python.
- Dịch bằng Google Translate công khai (miễn phí) hoặc NLLB-200 offline.
- CHỐNG THIẾU DỊCH: ánh xạ 1-1 từng câu; nhận diện đúng bị-chặn
  (302/429/503/trang HTML) để mọi luồng cùng nghỉ nguội; khử trùng lặp;
  câu toàn ký hiệu giữ nguyên; tới 4 vòng thử lại; cache tái sử dụng.
- Mọi dữ liệu xử lý ngay trên máy bạn.
"""

import argparse
import json
import os
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "1.3.0"
DEFAULT_PORT = 8321
MAX_UPLOAD = 30 * 1024 * 1024  # 30 MB

_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

GTX_URL = "https://translate.googleapis.com/translate_a/single"
T_MAIN = "https://translate.googleapis.com/translate_a/t?"   # host chính
T_ALT = "https://clients5.google.com/translate_a/t?"          # host dự phòng

# ---- engine offline (NLLB-200 qua CTranslate2, int8, ~650MB) ----
NLLB_REPO = "JustFrederik/nllb-200-distilled-600M-ct2-int8"
NLLB_LANGS = {
    "ja": "jpn_Jpan", "vi": "vie_Latn", "en": "eng_Latn",
    "zh": "zho_Hans", "ko": "kor_Hang", "th": "tha_Thai",
    "id": "ind_Latn", "fr": "fra_Latn", "es": "spa_Latn",
    "pt": "por_Latn", "ru": "rus_Cyrl", "ar": "arb_Arab", "hi": "hin_Deva",
}
LOCAL_PIP = "pip install ctranslate2 sentencepiece huggingface_hub"

# ---- cache dịch trên đĩa ----
CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "srt2json_cache.json")


# ===========================================================================
# 1) PHÂN TÍCH SRT / VTT  —  đọc timestamp + tách từng câu
# ===========================================================================

# Nhận cả "HH:MM:SS,mmm" (SRT) lẫn "MM:SS.mmm" / "HH:MM:SS.mmm" (VTT)
_TS = r"(?:(\d+):)?(\d{1,2}):(\d{2})[.,](\d{1,3})"
CUE_RE = re.compile(r"^\s*" + _TS + r"\s*-->\s*" + _TS)

_SENT_SPLIT_RE = re.compile(r"(?<=[。！？!?…])\s*")

# "Câu thật" = có chữ cái/kana/kanji/hangul. Câu chỉ toàn ký hiệu
# (♪, …, ー, -, 123...) được giữ nguyên, không cần dịch.
_REAL_TEXT_RE = re.compile(
    r"[A-Za-z\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uff66-\uff9f\uac00-\ud7af]")


class CancelledError(Exception):
    """Người dùng đã huỷ tác vụ."""


def _to_sec(h, m, s, ms):
    ms = (ms or "0").ljust(3, "0")
    return int(h or 0) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def clean_text(t, keep_tags=False):
    """Làm sạch 1 câu: bỏ thẻ ASS {\\an8}, thẻ HTML <i>..., gộp khoảng trắng."""
    if not keep_tags:
        t = re.sub(r"\{\\[^}]*\}", "", t)   # {\an8}, {\i1}...
        t = re.sub(r"<[^>]+>", "", t)       # <i>, <font ...>...
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def parse_subtitle(text, keep_tags=False):
    """Đọc SRT/VTT -> danh sách cue: {"s": giây, "e": giây, "src": câu}.

    - Chịu cả BOM, CRLF, dòng số thứ tự, cue-id của VTT, NOTE/STYLE của VTT.
    - Cue nhiều dòng được ghép thành 1 câu (nối bằng dấu cách).
    """
    norm = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
    lines = norm.split("\n")
    cues = []
    i, n = 0, len(lines)
    while i < n:
        m = CUE_RE.match(lines[i])
        if not m:
            i += 1
            continue
        g = m.groups()
        start = _to_sec(g[0], g[1], g[2], g[3])
        end = _to_sec(g[4], g[5], g[6], g[7])
        i += 1
        buf = []
        while i < n and lines[i].strip():
            buf.append(lines[i].strip())
            i += 1
        content = clean_text(" ".join(buf), keep_tags)
        if content:
            cues.append({"s": round(start, 2), "e": round(end, 2),
                         "src": content, "tgt": ""})
    return cues


def split_sentences(text):
    """Tách 1 chuỗi thành các câu theo 。！？!?…"""
    parts = [p.strip() for p in _SENT_SPLIT_RE.split(text)]
    return [p for p in parts if p]


def split_long_cues(cues):
    """Tách cue chứa nhiều câu thành nhiều cue, chia thời gian theo độ dài."""
    out = []
    for c in cues:
        parts = split_sentences(c["src"]) if c["e"] > c["s"] else [c["src"]]
        if len(parts) < 2:
            out.append(c)
            continue
        total = sum(len(p) for p in parts) or 1
        dur = c["e"] - c["s"]
        t = c["s"]
        for k, p in enumerate(parts):
            if k == len(parts) - 1:
                e = c["e"]
            else:
                e = round(c["s"] + dur * (sum(len(x) for x in parts[:k + 1]) / total), 2)
            if e <= t:  # không để thời gian lặp/âm
                e = t
            out.append({"s": round(t, 2), "e": e, "src": p, "tgt": ""})
            t = e
    return out


def load_fix_json(text, src="ja", tgt="vi"):
    """Đọc JSON đầu ra cũ -> cues (chế độ điền nốt câu thiếu). None nếu sai."""
    try:
        data = json.loads(text)
    except ValueError:
        return None
    if not (isinstance(data, dict) and isinstance(data.get("cues"), list)
            and data["cues"]):
        return None
    cues = []
    for c in data["cues"]:
        if not isinstance(c, dict) or src not in c:
            return None
        s = c.get("s")
        if not isinstance(s, (int, float)):
            return None
        e = c.get("e", c.get("s"))
        if not isinstance(e, (int, float)):
            e = s
        cues.append({"s": s, "e": e,
                     "src": str(c[src]),
                     "tgt": str(c.get(tgt, "") or "")})
    return cues


def dump_json(cues, src="ja", tgt="vi"):
    """Xuất JSON theo đúng định dạng: {"cues":[{"s","e","ja","vi"}]}."""
    return json.dumps(
        {"cues": [{"s": c["s"], "e": c["e"],
                   src: c["src"], tgt: c.get("tgt", "")} for c in cues]},
        ensure_ascii=False, indent=2)


def decode_bytes(data):
    """Đoán encoding: UTF-8 (có/không BOM), UTF-16, Shift-JIS, EUC-JP..."""
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        try:
            return data.decode("utf-16")
        except UnicodeDecodeError:
            pass
    for enc in ("utf-8-sig", "utf-8", "shift_jis", "euc-jp"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.decode("utf-8", errors="replace")


# ===========================================================================
# 2) CACHE DỊCH TRÊN ĐĨA — câu đã dịch không bao giờ dịch lại
# ===========================================================================

_cache_lock = threading.Lock()
_cache = None  # {"ja:vi": {"câu nhật": "câu việt", ...}}


def _cache_locked():
    """Nạp cache lần đầu. PHẢI đang giữ _cache_lock."""
    global _cache
    if _cache is None:
        _cache = {}
        try:
            with open(CACHE_PATH, encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict):
                _cache = d
        except Exception:
            _cache = {}
    return _cache


def _cache_get(src, tgt, text):
    try:
        with _cache_lock:
            return _cache_locked().get("%s:%s" % (src, tgt), {}).get(text) or ""
    except Exception:
        return ""


def _cache_put_many(src, tgt, pairs):
    with _cache_lock:
        d = _cache_locked().setdefault("%s:%s" % (src, tgt), {})
        for t, v in pairs:
            if t and v:
                d[t] = v


def _cache_save():
    with _cache_lock:
        if _cache is None:
            return
        try:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(_cache, f, ensure_ascii=False)
        except Exception:
            pass


# ===========================================================================
# 3) ENGINE GOOGLE (online, miễn phí, không cần key)
#    + Tầng 1: GET translate_a/t — mỗi câu 1 tham số "q" => kết quả ĐÚNG
#      VỊ TRÍ 1-1. 2 host xen kẽ (translate.googleapis.com / clients5).
#    + Tầng 2: POST gtx — ghép lô bằng "\n"; lệch dòng -> chia đôi đệ quy.
#    + Tầng 3: dịch từng câu một.
#    + NHẬN DIỆN BỊ CHẶN (302 -> trang "sorry", 429, 503, HTML 200):
#      KHÔNG theo redirect; khi bị chặn, mọi luồng cùng nghỉ (_Throttle)
#      với thời gian tăng dần. Đang bị chặn nặng thì bỏ lô ngay.
#    + Nhịp nhẹ (min_gap) giữa 2 request để ÍT BỊ CHẶN ngay từ đầu.
# ===========================================================================

class _Mismatch(Exception):
    """Endpoint trả số kết quả khác số câu gửi — phải chia nhỏ lại."""


class _Blocked(Exception):
    """Bị Google chặn tạm (rate limit: 302/429/503/trang HTML sorry)."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Từ chối theo redirect: 302 sang trang sorry = dấu hiệu bị chặn."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # buộc urllib nêu HTTPError cho mã 3xx


_OPENER = urllib.request.build_opener(_NoRedirect())
_BLOCK_CODES = (301, 302, 303, 307, 308, 403, 429, 503)


def _fetch(req, timeout=25):
    """Thực hiện request KHÔNG theo redirect; nhận diện bị chặn."""
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code in _BLOCK_CODES:
            raise _Blocked("HTTP %d" % e.code)
        raise
    if body.lstrip()[:1] == "<":  # nhận HTML thay vì JSON => trang "sorry"
        raise _Blocked("phản hồi HTML")
    return body


class _Throttle(object):
    """Nhịp chung cho engine google:
    (1) mọi request cách nhau tối thiểu min_gap (đỡ bị chặn);
    (2) khi bị chặn, mọi luồng cùng tạm nghỉ cho giới hạn nguội."""

    def __init__(self, min_gap=0.2):
        self._lock = threading.Lock()
        self._until = 0.0       # nghỉ tới lúc này (phạt bị chặn)
        self._next_start = 0.0  # mốc sớm nhất cho request kế tiếp
        self._min_gap = max(0.0, min_gap)

    def penalty(self, seconds):
        with self._lock:
            self._until = max(self._until, time.time() + seconds)

    def wait(self):
        """Đặt chỗ 1 lượt rồi chờ tới lượt đó (KHÔNG đặt lại khi chờ,
        kẻo lượt bị tự đẩy ra xa vô hạn). Nếu đang có phạt (bị chặn)
        thì chờ luôn cho hết phạt."""
        with self._lock:
            now = time.time()
            start = max(now, self._next_start, self._until)
            self._next_start = start + self._min_gap
        while True:
            with self._lock:
                d = max(start, self._until) - time.time()
            if d <= 0:
                return
            time.sleep(min(d, 1.0))

    def blocked(self):
        """True nếu đang trong đợt chặn nặng (>4s còn lại) — nên bỏ lô."""
        with self._lock:
            return (self._until - time.time()) > 4.0


def _gtx_post(q, src, tgt):
    """POST tới translate_a/single (client=gtx). Trả về chuỗi đã dịch."""
    data = urllib.parse.urlencode(
        {"client": "gtx", "sl": src, "tl": tgt, "dt": "t", "q": q}
    ).encode("utf-8")
    req = urllib.request.Request(GTX_URL, data=data,
                                 headers={"User-Agent": random.choice(_UAS)})
    resp = json.loads(_fetch(req, timeout=30))
    segs = resp[0] if resp and resp[0] else []
    return "".join(s[0] for s in segs if s and s[0])


def _t_get(qlist, src, tgt, base):
    """GET translate_a/t (client=dict-chrome-ex): N tham số q -> N kết quả.

    Ưu điểm: kết quả nằm đúng vị trí trong mảng -> ánh xạ 1-1 chính xác.
    """
    params = [("client", "dict-chrome-ex"), ("sl", src), ("tl", tgt)]
    for q in qlist:
        params.append(("q", q))
    req = urllib.request.Request(base + urllib.parse.urlencode(params),
                                 headers={"User-Agent": random.choice(_UAS)})
    resp = json.loads(_fetch(req, timeout=25))
    if not isinstance(resp, list):
        raise ValueError("translate_a/t trả định dạng lạ")
    out = []
    for it in resp:
        if isinstance(it, list):
            it = it[0] if it else ""
        if not isinstance(it, str):
            raise ValueError("translate_a/t trả phần tử lạ")
        out.append(it)
    if len(out) != len(qlist):
        raise _Mismatch("translate_a/t: %d != %d" % (len(out), len(qlist)))
    return out


def _t_url_len(qlist):
    """Ước lượng độ dài URL khi ghép qlist vào tham số q."""
    n = 140  # phần cố định của URL
    for q in qlist:
        n += len(urllib.parse.quote(q, safe="")) + 3
    return n


def _with_retries(fn, tries=4, throttle=None):
    """Chạy fn với retry. PHÂN BIỆT 2 loại lỗi:
    - _Blocked (bị chặn): nộp phạt toàn cục, nghỉ nguội tăng dần;
    - lỗi thường: backoff ngắn rồi thử lại.
    """
    last = None
    for attempt in range(tries):
        if throttle:
            throttle.wait()
        try:
            return fn()
        except _Mismatch:
            raise
        except CancelledError:
            raise
        except _Blocked as e:
            last = e
            if throttle:
                throttle.penalty(min(30.0, 1.5 * (attempt + 1) + 2.0))
            time.sleep(min(10.0, 1.0 + attempt) + random.random() * 0.5)
        except Exception as e:
            last = e
            time.sleep(min(6.0, 0.4 * (2 ** attempt)) + random.random() * 0.3)
    raise last or RuntimeError("không thành công")


def _t_multi_any(texts, src, tgt, throttle):
    """Tầng 1: translate_a/t — xen kẽ 2 host qua các lần thử."""
    state = {"n": 0}

    def fn():
        base = (T_MAIN, T_ALT)[state["n"] % 2]
        state["n"] += 1
        return _t_get(texts, src, tgt, base)

    return _with_retries(fn, tries=4, throttle=throttle)


def _gtx_join(texts, src, tgt):
    """Ghép cả lô bằng "\\n" rồi POST 1 lần. Lệch dòng -> nêu _Mismatch."""
    out = _gtx_post("\n".join(texts), src, tgt)
    lines = out.split("\n")
    if len(lines) != len(texts):
        raise _Mismatch("gtx: %d != %d" % (len(lines), len(texts)))
    return [l.strip() for l in lines]


def _gtx_join_split(texts, src, tgt, throttle):
    """gtx ghép dòng; nếu Google gộp dòng -> chia đôi đệ quy tìm đúng câu."""
    try:
        return _with_retries(lambda: _gtx_join(texts, src, tgt), tries=2,
                             throttle=throttle)
    except _Mismatch:
        if len(texts) == 1:
            return [_with_retries(lambda: _gtx_post(texts[0], src, tgt),
                                  tries=3, throttle=throttle)]
        half = len(texts) // 2
        return (_gtx_join_split(texts[:half], src, tgt, throttle) +
                _gtx_join_split(texts[half:], src, tgt, throttle))


def _split_for_len(text, maxlen):
    """Câu đơn quá dài -> cắt tại ranh giới câu để vừa giới hạn request."""
    parts = re.split(r"(?<=[。！？!?…\n])", text)
    chunks, cur = [], ""
    for p in parts:
        if not cur or len(cur) + len(p) <= maxlen:
            cur += p
        else:
            chunks.append(cur)
            cur = p
    if cur:
        chunks.append(cur)
    final = []
    for c in chunks:
        while len(c) > maxlen:      # cực hiếm: 1 câu khổng lồ không có dấu câu
            final.append(c[:maxlen])
            c = c[maxlen:]
        if c:
            final.append(c)
    return final


def _translate_one(text, src, tgt, throttle):
    """Dịch 1 câu bằng mọi cách: gtx POST -> translate_a/t (2 host)."""
    if len(text) > 1200:
        return "".join(_translate_one(p, src, tgt, throttle)
                       for p in _split_for_len(text, 1200))
    try:
        return _with_retries(lambda: _gtx_post(text, src, tgt), tries=3,
                             throttle=throttle).replace("\n", " ").strip()
    except Exception:
        pass
    for base in (T_MAIN, T_ALT):
        try:
            r = _with_retries(lambda b=base: _t_get([text], src, tgt, b),
                              tries=2, throttle=throttle)
            return (r[0] or "").replace("\n", " ").strip()
        except Exception:
            continue
    raise RuntimeError("không dịch được")


def _translate_batch(texts, src, tgt, cancel, throttle):
    """Dịch 1 lô: 3 tầng dự phòng. Đang bị chặn nặng -> trả rỗng NGAY
    để vòng sau (sau khi nguội) thử lại, thay vì dồn request vào chỗ chặn."""
    if cancel and cancel():
        raise CancelledError()

    # ---- Tầng 1: translate_a/t nhiều q — nhanh + ánh xạ chính xác 1-1 ----
    if not throttle.blocked() and _t_url_len(texts) <= 1900:
        try:
            return _t_multi_any(texts, src, tgt, throttle)
        except _Mismatch:
            pass
        except CancelledError:
            raise
        except Exception:
            pass

    # ---- Tầng 2: gtx ghép dòng (tự chia đôi khi lệch) ----
    if not throttle.blocked():
        try:
            out = _gtx_join_split(texts, src, tgt, throttle)
            if len(out) == len(texts):
                return out
        except CancelledError:
            raise
        except Exception:
            pass

    # ---- Tầng 3: từng câu một ----
    out = []
    for t in texts:
        if cancel and cancel():
            raise CancelledError()
        if throttle.blocked():
            out.append("")
            continue
        try:
            out.append(_translate_one(t, src, tgt, throttle))
        except Exception:
            out.append("")
        time.sleep(0.1)
    return out


def _build_batches(texts, todo, max_url=1500, max_items=40):
    """Chia câu thành các lô vừa URL của translate_a/t."""
    batches, cur, curlen = [], [], 0
    for i in todo:
        enc = len(urllib.parse.quote(texts[i], safe="")) + 3
        if cur and (curlen + enc > max_url or len(cur) >= max_items):
            batches.append(cur)
            cur, curlen = [], 0
        cur.append(i)
        curlen += enc
    if cur:
        batches.append(cur)
    return batches


# ===========================================================================
# 4) ENGINE OFFLINE — NLLB-200 (CTranslate2 int8, dịch thẳng Nhật→Việt)
#    Cài 1 lần:  pip install ctranslate2 sentencepiece huggingface_hub
#    Model ~650MB tự tải lần đầu vào models/nllb600/ cạnh script.
#    (Ghi đè thư mục model bằng biến môi trường SRT2JSON_MODEL_DIR.)
# ===========================================================================

_local_state = {"lock": threading.Lock(), "translator": None, "sp": None}


def _local_model_dir():
    return os.environ.get("SRT2JSON_MODEL_DIR") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "models", "nllb600")


def _local_ready():
    """Kiểm tra thư viện offline đã cài chưa. Trả về (ok, thông báo lỗi)."""
    missing = []
    for mod in ("ctranslate2", "sentencepiece", "huggingface_hub"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        return False, ("Chưa cài thư viện cho chế độ Offline. Chạy lệnh:\n"
                       "pip install ctranslate2 sentencepiece huggingface_hub\n"
                       "(còn thiếu: %s)" % ", ".join(missing))
    return True, None


def _local_model_downloaded():
    return os.path.exists(os.path.join(_local_model_dir(), "model.bin"))


def _ensure_local(on_status=None):
    """Nạp model NLLB (tự tải lần đầu ~650MB). Trả về (translator, sp)."""
    with _local_state["lock"]:
        if _local_state["translator"] is not None:
            return _local_state["translator"], _local_state["sp"]
        import ctranslate2
        import sentencepiece as spm
        from huggingface_hub import snapshot_download

        mdir = _local_model_dir()
        if not os.path.exists(os.path.join(mdir, "model.bin")):
            if on_status:
                on_status()
            snapshot_download(NLLB_REPO, local_dir=mdir)
        if on_status:
            on_status()
        translator = ctranslate2.Translator(mdir, device="cpu")
        sp = spm.SentencePieceProcessor(
            model_file=os.path.join(mdir, "sentencepiece.bpe.model"))
        _local_state["translator"] = translator
        _local_state["sp"] = sp
        return translator, sp


def _nllb_chunk(texts, src, tgt):
    """Dịch 1 nhóm câu bằng NLLB offline (src/tgt là mã 2 chữ: ja, vi...)."""
    sl = NLLB_LANGS.get(src)
    tl = NLLB_LANGS.get(tgt)
    if not sl or not tl:
        raise RuntimeError("Offline chưa hỗ trợ cặp %s→%s (hỗ trợ: %s)"
                           % (src, tgt, ", ".join(sorted(NLLB_LANGS))))
    translator, sp = _ensure_local()
    enc = [[sl] + sp.encode(t, out_type=str) + ["</s>"] for t in texts]
    res = translator.translate_batch(enc, target_prefix=[[tl]] * len(texts),
                                     beam_size=1, max_batch_size=8)
    outs = []
    for r in res:
        pieces = [p for p in r.hypotheses[0][1:]
                  if p not in ("</s>", "<s>", "<unk>")]
        outs.append(sp.decode(pieces).strip())
    return outs


def _local_translate_list(texts, src, tgt, on_progress=None, cancel=None):
    """Dịch offline danh sách câu (theo nhóm 32 câu, báo tiến độ)."""
    results = [""] * len(texts)
    flat = []  # (chỉ số trong texts, đoạn con) — câu dài được cắt nhỏ
    for i, t in enumerate(texts):
        if len(t) > 600:
            for p in _split_for_len(t, 600):
                flat.append((i, p))
        else:
            flat.append((i, t))
    done = 0
    B = 32
    for c0 in range(0, len(flat), B):
        if cancel and cancel():
            raise CancelledError()
        group = flat[c0:c0 + B]
        outs = _nllb_chunk([t for _, t in group], src, tgt)
        for (i, _), v in zip(group, outs):
            v = (v or "").strip()
            results[i] = (results[i] + v).strip() if results[i] else v
        done += len(group)
        if on_progress:
            on_progress(done, len(flat))
    return results


# ===========================================================================
# 5) HÀM DỊCH CHÍNH — dùng cho cả web lẫn CLI
#    - Nhanh: chia lô + song song + khử trùng + bỏ câu ký hiệu + cache đĩa
#    - Đủ (google): 4 vòng thử lại sau khi chờ nguội
#    - on_progress(done, total, note) — note: "retry" | "model" | None
# ===========================================================================

def translate_texts(texts, src="ja", tgt="vi", on_progress=None,
                    on_last=None, cancel=None, workers=5,
                    engine="google", use_cache=True):
    """Dịch danh sách câu -> danh sách đã dịch (đúng thứ tự)."""
    results = [""] * len(texts)
    todo = [i for i, t in enumerate(texts) if t and t.strip()]
    if not todo:
        return results

    # ---- 0) Câu chỉ toàn ký hiệu/số (♪, …, ー, 123): giữ nguyên ----
    for i in todo:
        if not _REAL_TEXT_RE.search(texts[i]):
            results[i] = texts[i]
    todo = [i for i in todo if _REAL_TEXT_RE.search(texts[i])]
    if not todo:
        return results

    # ---- 1) Khử trùng lặp: câu giống nhau chỉ dịch 1 lần ----
    memo = {}                 # câu -> bản dịch ("" = chưa có)
    uniq = []
    for i in todo:
        t = texts[i]
        if t not in memo:
            memo[t] = ""
            uniq.append(t)

    # ---- 2) Lấy từ cache trên đĩa: câu từng dịch rồi không dịch lại ----
    if use_cache:
        for t in uniq:
            v = _cache_get(src, tgt, t)
            if v:
                memo[t] = v

    lock = threading.Lock()
    counter = {"done": sum(1 for t in uniq if memo[t]), "total": len(uniq)}

    def report(note=None):
        if on_progress:
            with lock:
                d, t = counter["done"], counter["total"]
            on_progress(d, t, note)

    report()

    # ================== ENGINE OFFLINE (NLLB) ==================
    if engine == "local":
        pending = [j for j, t in enumerate(uniq) if not memo[t]]
        if pending:
            def _status():
                report(note="model")  # đang tải/nạp model lần đầu

            def lp(done, total):
                if on_progress:
                    on_progress(min(done, len(pending)), len(pending), None)

            outs = _local_translate_list([uniq[j] for j in pending],
                                         src, tgt, on_progress=lp,
                                         cancel=cancel)
            for j, v in zip(pending, outs):
                if v:
                    memo[uniq[j]] = v
            with lock:
                counter["done"] = sum(1 for t in uniq if memo[t])
            report()
        if use_cache:
            _cache_put_many(src, tgt,
                            [(t, memo[t]) for t in uniq if memo[t]])
            _cache_save()
        for i in todo:
            results[i] = memo.get(texts[i], "")
        return results

    # ================== ENGINE GOOGLE (online) ==================
    workers = max(1, min(12, int(workers or 5)))
    throttle = _Throttle(min_gap=0.2)

    def work(idxs, note=None):
        if cancel and cancel():
            raise CancelledError()
        batch = [uniq[j] for j in idxs]
        out = _translate_batch(batch, src, tgt, cancel, throttle)
        if len(out) != len(idxs):  # phòng hờ
            out = (out + [""] * len(idxs))[:len(idxs)]
        with lock:
            for k, j in enumerate(idxs):
                v = (out[k] or "").strip()
                if v:
                    memo[uniq[j]] = v
            counter["done"] = sum(1 for t in uniq if memo[t])
        if on_last and idxs:
            j = idxs[-1]
            on_last(uniq[j], memo[uniq[j]])
        report(note)
        time.sleep(0.05 + random.random() * 0.05)

    def run_pass(idxs, w, note):
        batches = _build_batches(uniq, idxs)
        if w <= 1 or len(batches) == 1:
            for b in batches:
                work(b, note)
        else:
            with ThreadPoolExecutor(max_workers=w) as pool:
                futures = [pool.submit(work, b, note) for b in batches]
                for f in futures:
                    f.result()

    # ---- Vòng 1: full tốc độ ----
    missing = [j for j, t in enumerate(uniq) if not memo[t]]
    if missing:
        run_pass(missing, workers, None)

        # ---- Vòng 2-4: chờ nguội rồi chỉ lấy lại câu còn trống ----
        for wait_s in (2.0, 8.0, 20.0):
            bad = [j for j, t in enumerate(uniq) if not memo[t]]
            if not bad:
                break
            time.sleep(wait_s)
            throttle.wait()
            report(note="retry")
            run_pass(bad, max(1, workers // 2), "retry")

    if use_cache:
        _cache_put_many(src, tgt,
                        [(t, memo[t]) for t in uniq if memo[t]])
        _cache_save()

    for i in todo:
        results[i] = memo.get(texts[i], "")
    return results


# ===========================================================================
# 6) JOB (cho web) — trạng thái dịch để UI poll tiến độ
# ===========================================================================

_JOBS = {}
_JOBS_LOCK = threading.Lock()
_JOB_TTL = 3 * 3600


class Job(object):
    def __init__(self, cues, name, src, tgt, workers=5, fix=False,
                 engine="google", use_cache=True):
        self.id = uuid.uuid4().hex[:12]
        self.cues = cues
        self.name = name
        self.src = src
        self.tgt = tgt
        self.workers = workers
        self.fix = fix
        self.engine = engine
        self.use_cache = use_cache
        self.status = "translating"
        self.total = len(cues)
        self.done = 0
        self.failed = 0
        self.error = None
        self.last = None
        self.note = None
        self.cancel = False
        self.created = time.time()
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return {"status": self.status, "total": self.total,
                    "done": self.done, "failed": self.failed,
                    "error": self.error, "last": self.last, "note": self.note}


def _cleanup_jobs():
    now = time.time()
    for jid in [k for k, v in _JOBS.items() if now - v.created > _JOB_TTL]:
        _JOBS.pop(jid, None)


def _run_job(job):
    try:
        def on_progress(done, total, note=None):
            with job.lock:
                job.done = done
                job.total = total
                job.note = note

        def on_last(src_text, tgt_text):
            with job.lock:
                job.last = {"ja": src_text, "vi": tgt_text}

        if job.fix:
            # ---- chế độ điền nốt: chỉ dịch các câu còn trống ----
            missing = [i for i, c in enumerate(job.cues) if not c["tgt"]]
            with job.lock:
                job.total = len(missing) or 1
                job.done = 0
            if not missing:
                with job.lock:
                    job.status = "done"
                return
            results = translate_texts(
                [job.cues[i]["src"] for i in missing], job.src, job.tgt,
                on_progress=on_progress, on_last=on_last,
                cancel=lambda: job.cancel, workers=job.workers,
                engine=job.engine, use_cache=job.use_cache)
            with job.lock:
                for k, i in enumerate(missing):
                    job.cues[i]["tgt"] = results[k]
                job.failed = sum(1 for c in job.cues if not c["tgt"])
                job.status = "done"
        else:
            results = translate_texts(
                [c["src"] for c in job.cues], job.src, job.tgt,
                on_progress=on_progress, on_last=on_last,
                cancel=lambda: job.cancel, workers=job.workers,
                engine=job.engine, use_cache=job.use_cache)
            with job.lock:
                for c, v in zip(job.cues, results):
                    c["tgt"] = v
                job.failed = sum(1 for c in job.cues if not c["tgt"])
                job.status = "done"
    except CancelledError:
        with job.lock:
            job.status = "cancelled"
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = ("%s: %s" % (type(e).__name__, e)) or "lỗi không rõ"


# ===========================================================================
# 7) FILE MẪU
# ===========================================================================

SAMPLE_SRT = """1
00:00:47,290 --> 00:00:49,790
（彼氏）はい

2
00:00:50,120 --> 00:00:53,400
今日は本当にいい天気ですね。
散歩に行きましょうよ。

3
00:00:54,000 --> 00:00:56,500
うん、そうだね。

4
00:00:57,000 --> 00:01:00,250
<i>しかし、彼は何も知らなかった…</i>

5
00:01:01,000 --> 00:01:03,750
ちょっと待って！今行く！

6
00:01:04,500 --> 00:01:08,000
これはテスト用のサンプル字幕です。日本語からベトナム語へ翻訳されます。
"""


# ===========================================================================
# 8) GIAO DIỆN WEB (HTML/CSS/JS nhúng — không cần internet cho giao diện)
# ===========================================================================

PAGE = r"""<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SRT/VTT → JSON · Dịch Nhật → Việt</title>
<style>
:root{
  --bg:#0b1120;--card:#121b30;--card2:#0d1526;--line:#223050;
  --text:#e7eefb;--muted:#93a5c4;--accent:#7c8cff;--accent2:#b06cff;
  --ok:#3ddc97;--err:#ff6b6b;--warn:#ffc24b;
}
*{box-sizing:border-box}
[hidden]{display:none!important}
html,body{margin:0}
body{
  font-family:"Segoe UI",system-ui,-apple-system,"Hiragino Sans","Noto Sans",sans-serif;
  background:radial-gradient(1100px 600px at 85% -10%,#1d2c50 0%,transparent 60%),
             radial-gradient(900px 500px at -10% 110%,#251b4a 0%,transparent 55%),var(--bg);
  color:var(--text);min-height:100vh;
}
.wrap{max-width:980px;margin:0 auto;padding:30px 16px 70px}
header{text-align:center;margin-bottom:22px}
.logo{font-size:40px;line-height:1}
h1{font-size:26px;margin:10px 0 6px;letter-spacing:.3px}
h1 .arrow{color:var(--accent)}
.sub{color:var(--muted);margin:0;font-size:14px}
.badge{display:inline-block;padding:1px 8px;border-radius:6px;font-size:11px;font-weight:700;vertical-align:1px}
.b-ja{background:#d84040;color:#fff}
.b-vi{background:#2f8f4e;color:#fff}
.card{background:linear-gradient(180deg,var(--card),var(--card2));border:1px solid var(--line);border-radius:16px;padding:20px;margin-bottom:18px;box-shadow:0 10px 30px rgba(0,0,0,.25)}
.tabs{display:flex;gap:8px;margin-bottom:14px}
.tab{flex:1;background:#0a1120;border:1px solid var(--line);color:var(--muted);padding:9px 12px;border-radius:10px;font-size:14px;cursor:pointer;transition:.15s}
.tab.active{color:var(--text);border-color:var(--accent);background:#152040}
.drop{border:2px dashed #33456e;border-radius:14px;padding:34px 16px;text-align:center;cursor:pointer;transition:.15s;background:rgba(20,30,55,.35)}
.drop:hover,.drop.over{border-color:var(--accent);background:rgba(30,45,80,.5)}
.drop-ico{font-size:34px}
.drop-t{font-size:15px;margin-top:6px}
.drop-s{color:var(--muted);font-size:12.5px;margin-top:4px}
.file-info{margin-top:10px;background:#0a1120;border:1px solid var(--line);border-radius:10px;padding:9px 12px;font-size:13.5px}
textarea{width:100%;height:170px;background:#0a1120;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:10px 12px;font:13px/1.5 Consolas,Menlo,monospace;resize:vertical}
textarea:focus,input:focus{outline:1px solid var(--accent)}
select{background:#0a1120;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:4px 8px;font-size:13px;margin-left:8px;vertical-align:middle}
.adv{margin:14px 0 4px;color:var(--muted);font-size:13.5px}
.adv summary{cursor:pointer;user-select:none}
.adv label{display:block;margin:9px 0 0 4px;cursor:pointer}
button{font-family:inherit}
.primary{display:block;width:100%;margin-top:14px;padding:13px;font-size:16px;font-weight:700;color:#fff;border:0;border-radius:12px;cursor:pointer;background:linear-gradient(90deg,var(--accent),var(--accent2));transition:.15s;box-shadow:0 6px 18px rgba(110,100,255,.35)}
.primary:hover{filter:brightness(1.1)}
.primary:disabled{opacity:.55;cursor:not-allowed}
.ghost{background:#0a1120;color:var(--text);border:1px solid var(--line);padding:7px 13px;border-radius:9px;font-size:13.5px;cursor:pointer;transition:.15s;margin-top:10px}
.ghost:hover{border-color:var(--accent)}
.ghost.sm{margin:0;padding:4px 10px;font-size:12.5px}
.err{margin-top:12px;background:rgba(255,80,80,.12);border:1px solid rgba(255,90,90,.4);color:#ffb3b3;padding:10px 12px;border-radius:10px;font-size:13.5px;white-space:pre-line}
.prog-head{display:flex;justify-content:space-between;align-items:center;font-size:15px;font-weight:600}
.bar{height:12px;background:#0a1120;border:1px solid var(--line);border-radius:99px;overflow:hidden;margin:13px 0 8px}
.bar>div{height:100%;width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));transition:width .3s}
.muted{color:var(--muted);font-size:13px}
.last{margin-top:12px;background:#0a1120;border:1px solid var(--line);border-radius:10px;padding:10px 12px;font-size:14px}
.ll-ja{color:var(--muted);font-size:13px;margin-bottom:4px}
.ll-vi{color:var(--ok);font-weight:600}
.stats{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.chip{background:#0a1120;border:1px solid var(--line);border-radius:99px;padding:6px 14px;font-size:13px;color:var(--muted)}
.chip b{color:var(--text);margin-left:4px}
.chip.ok b{color:var(--ok)}
.chip.err b{color:var(--err)}
.actions{display:flex;gap:10px;flex-wrap:wrap}
.actions .primary{width:auto;margin:0;padding:11px 22px}
.actions .ghost{margin:0}
.ok-msg{color:var(--ok);margin-top:10px;font-size:13.5px}
.tbl-wrap{margin-top:16px;border:1px solid var(--line);border-radius:12px;overflow:auto;max-height:440px}
table{border-collapse:collapse;width:100%;font-size:13px;min-width:640px}
th{position:sticky;top:0;background:#0e1830;color:var(--muted);text-align:left;padding:9px 10px;font-size:12px;letter-spacing:.4px;border-bottom:1px solid var(--line)}
td{padding:8px 10px;border-bottom:1px solid #1a2740;vertical-align:top}
tr:nth-child(even) td{background:rgba(255,255,255,.015)}
td.num{color:var(--accent);font-family:Consolas,Menlo,monospace;white-space:nowrap;font-size:12.5px}
td.ja{color:#dbe4f5;width:34%}
td.vi{color:#c9f7e2}
tr.fail td{background:rgba(255,194,75,.07)}
tr.fail td.vi{color:var(--warn)}
footer{text-align:center;color:var(--muted);font-size:12.5px;margin-top:26px}
.overlay{position:fixed;inset:0;background:rgba(3,7,18,.8);display:flex;align-items:center;justify-content:center;padding:20px;z-index:50}
.overlay .box{width:min(880px,100%);max-height:82vh;display:flex;flex-direction:column;background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:hidden}
.overlay .box-head{display:flex;justify-content:space-between;align-items:center;padding:12px 14px;border-bottom:1px solid var(--line);font-weight:600}
.overlay textarea{flex:1;min-height:52vh;border:0;border-radius:0}
.spin{display:inline-block;width:14px;height:14px;border:2px solid rgba(255,255,255,.4);border-top-color:#fff;border-radius:50%;animation:sp .7s linear infinite;vertical-align:-2px;margin-right:6px}
@keyframes sp{to{transform:rotate(360deg)}}
@media(max-width:600px){h1{font-size:21px}.actions{flex-direction:column}.actions .primary{width:100%}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">🎬</div>
    <h1>SRT / VTT <span class="arrow">→</span> JSON</h1>
    <p class="sub">Đọc timestamp · Tách từng câu · Dịch
      <span class="badge b-ja">JA</span> → <span class="badge b-vi">VI</span>
      · Google (online) hoặc NLLB (offline)</p>
  </header>

  <section class="card" id="inputCard">
    <div class="tabs">
      <button class="tab active" data-tab="file" type="button">📄 Tải file</button>
      <button class="tab" data-tab="paste" type="button">📋 Dán văn bản</button>
    </div>

    <div id="tab-file">
      <div id="drop" class="drop">
        <div class="drop-ico">⬇️</div>
        <div class="drop-t">Kéo thả file <b>.srt</b> / <b>.vtt</b> vào đây, hoặc bấm để chọn</div>
        <div class="drop-s">Hỗ trợ cả <b>JSON cũ còn câu thiếu</b> (tự điền nốt) · UTF-8 / UTF-16 / Shift-JIS</div>
        <button class="ghost" id="btnSample" type="button">✨ Thử với file mẫu</button>
      </div>
      <input type="file" id="fileInput" accept=".srt,.vtt,.txt,.json" hidden>
      <div id="fileInfo" class="file-info" hidden></div>
    </div>

    <div id="tab-paste" hidden>
      <textarea id="pasteBox" placeholder="Dán nội dung SRT/VTT (hoặc JSON còn câu thiếu) vào đây…" spellcheck="false"></textarea>
    </div>

    <details class="adv">
      <summary>⚙️ Tuỳ chọn nâng cao</summary>
      <label>Engine dịch:
        <select id="optEngine">
          <option value="google" selected>Google (online) — nhanh, chất lượng tốt</option>
          <option value="local">Offline (NLLB) — không cần mạng, không bị chặn, tải model 1 lần ~650MB</option>
        </select>
      </label>
      <label><input type="checkbox" id="optSplit"> Tách câu dài thành nhiều câu theo dấu 。！？ (chia lại thời gian theo độ dài)</label>
      <label><input type="checkbox" id="optTags"> Giữ thẻ định dạng (&lt;i&gt;, &lt;font&gt;…)</label>
      <label>Tốc độ dịch Google (số luồng song song):
        <select id="optWorkers">
          <option value="2">2 — chậm mà chắc (đang bị chặn thì dùng này)</option>
          <option value="5" selected>5 — cân bằng (mặc định)</option>
          <option value="8">8 — nhanh nhất</option>
        </select>
      </label>
    </details>

    <button id="btnStart" class="primary" type="button">🚀 Bắt đầu tách &amp; dịch</button>
    <div id="err" class="err" hidden></div>
  </section>

  <section class="card" id="progCard" hidden>
    <div class="prog-head">
      <div id="progLabel">Đang dịch…</div>
      <button class="ghost sm" id="btnCancel" type="button">✕ Huỷ</button>
    </div>
    <div class="bar"><div id="barFill"></div></div>
    <div id="progSub" class="muted"></div>
    <div id="lastLine" class="last" hidden></div>
  </section>

  <section class="card" id="doneCard" hidden>
    <div class="stats">
      <div class="chip">Tổng số câu<b id="stTotal">0</b></div>
      <div class="chip ok">Dịch OK<b id="stOk">0</b></div>
      <div class="chip err" id="stErrWrap">Bị lỗi<b id="stErr">0</b></div>
    </div>
    <div class="actions">
      <button class="primary" id="btnDownload" type="button">⬇️ Tải JSON</button>
      <button class="ghost" id="btnCopy" type="button">📋 Copy JSON</button>
      <button class="ghost" id="btnRaw" type="button">👁 Xem JSON</button>
      <button class="ghost" id="btnAgain" type="button">↺ Dịch file khác</button>
    </div>
    <div id="copyMsg" class="ok-msg" hidden>✓ Đã copy JSON vào clipboard</div>
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>#</th><th>s</th><th>e</th><th>JA</th><th>VI</th></tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <div id="moreNote" class="muted" style="margin-top:10px"></div>
  </section>

  <footer>srt2json.py · chạy 100% trên máy bạn · Google (online) hoặc NLLB (offline) · Ctrl+C để tắt</footer>
</div>

<div class="overlay" id="rawOverlay" hidden>
  <div class="box">
    <div class="box-head"><span id="rawTitle">JSON</span><button class="ghost sm" id="btnRawClose" type="button">✕ Đóng</button></div>
    <textarea id="rawBox" readonly spellcheck="false"></textarea>
  </div>
</div>

<script>
var $=function(id){return document.getElementById(id)};
var jobId=null,resultText='',outName='subtitles.json',pollTimer=null,curName='';

function esc(s){return String(s).replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
function fmtSize(n){return n>1048576?(n/1048576).toFixed(1)+' MB':(n/1024).toFixed(1)+' KB'}
function showErr(m){var e=$('err');e.hidden=false;e.textContent=m}
function hideErr(){$('err').hidden=true}

/* ---- tab Tải file / Dán văn bản ---- */
Array.prototype.forEach.call(document.querySelectorAll('.tab'),function(t){
  t.addEventListener('click',function(){
    Array.prototype.forEach.call(document.querySelectorAll('.tab'),function(x){x.classList.remove('active')});
    t.classList.add('active');
    var isFile=t.getAttribute('data-tab')==='file';
    $('tab-file').hidden=!isFile;
    $('tab-paste').hidden=isFile;
  });
});

/* ---- kéo thả file ---- */
var file=null;
var drop=$('drop'),fileInput=$('fileInput');
drop.addEventListener('click',function(e){if(e.target.id!=='btnSample')fileInput.click()});
fileInput.addEventListener('change',function(){setFile(fileInput.files[0])});
['dragenter','dragover'].forEach(function(ev){drop.addEventListener(ev,function(e){e.preventDefault();e.stopPropagation();drop.classList.add('over')})});
['dragleave','drop'].forEach(function(ev){drop.addEventListener(ev,function(e){e.preventDefault();e.stopPropagation();drop.classList.remove('over')})});
drop.addEventListener('drop',function(e){setFile(e.dataTransfer.files[0])});
window.addEventListener('dragover',function(e){e.preventDefault()});
window.addEventListener('drop',function(e){e.preventDefault()});

function setFile(f){
  file=f;fileInput.value=f?f.name:'';
  var info=$('fileInfo');
  if(!f){info.hidden=true;info.textContent='';return}
  info.hidden=false;
  info.innerHTML='📄 <b>'+esc(f.name)+'</b> · '+fmtSize(f.size)+' · <span style="color:var(--ok)">sẵn sàng ✓</span>';
}

$('btnSample').addEventListener('click',function(e){
  e.stopPropagation();
  fetch('/api/sample').then(function(r){return r.text()}).then(function(t){
    try{setFile(new File([t],'sample.ja.srt',{type:'text/plain'}))}
    catch(err){$('pasteBox').value=t;document.querySelector('.tab[data-tab="paste"]').click()}
  }).catch(function(){showErr('Không tải được file mẫu.')});
});

/* ---- bắt đầu ---- */
$('btnStart').addEventListener('click',start);

function lockUI(on){
  var b=$('btnStart');
  b.disabled=on;
  b.innerHTML=on?'<span class="spin"></span>Đang xử lý…':'🚀 Bắt đầu tách &amp; dịch';
}

function start(){
  hideErr();
  var isFileMode=!$('tab-file').hidden;
  var proceed=function(body,name){
    curName=name;
    var qs=new URLSearchParams({
      name:name,
      split:$('optSplit').checked?'1':'0',
      tags:$('optTags').checked?'1':'0',
      workers:$('optWorkers').value,
      engine:$('optEngine').value
    });
    lockUI(true);
    $('doneCard').hidden=true;
    $('progCard').hidden=false;
    $('lastLine').hidden=true;
    setProgress(0,0,'Đang phân tích file…');
    fetch('/api/upload?'+qs.toString(),{method:'POST',body:body})
      .then(function(r){return r.json().then(function(d){if(!r.ok)throw new Error(d.error||('HTTP '+r.status));return d})})
      .then(function(d){
        jobId=d.job_id;
        outName=(d.name||'subtitles').replace(/\.(srt|vtt|txt|json)$/i,'')+'.json';
        setProgress(0,d.total,d.fix?'Đang điền nốt các câu thiếu…':'Đang dịch…');
        poll();
      })
      .catch(function(e){
        showErr('Lỗi: '+e.message);
        resetAfterRun();
      });
  };
  if(isFileMode){
    if(!file){showErr('Hãy chọn file .srt / .vtt trước (hoặc bấm “Thử với file mẫu”).');return}
    file.arrayBuffer().then(function(buf){proceed(buf,file.name)});
  }else{
    var t=$('pasteBox').value.trim();
    if(!t){showErr('Hãy dán nội dung phụ đề vào ô văn bản.');return}
    proceed(new TextEncoder().encode(t),'pasted.srt');
  }
}

function setProgress(done,total,label){
  var pct=total?Math.round(done*100/total):0;
  $('barFill').style.width=pct+'%';
  $('progLabel').innerHTML='<span class="spin"></span>'+esc(label);
  $('progSub').textContent=(curName?curName+' · ':'')+(total?(done+' / '+total+' câu · '+pct+'%'):'');
}

function poll(){
  fetch('/api/progress?job='+jobId).then(function(r){return r.json()}).then(function(p){
    if(p.status==='translating'||p.status==='queued'){
      var lbl='Đang dịch…';
      if(p.note==='retry') lbl='Đang dịch lại các câu còn lỗi (chờ hết bị chặn)…';
      else if(p.note==='model') lbl='Đang tải/nạp model offline lần đầu (~650MB, chỉ 1 lần)…';
      setProgress(p.done,p.total,lbl);
      if(p.last){
        $('lastLine').hidden=false;
        $('lastLine').innerHTML='<div class="ll-ja">'+esc(p.last.ja)+'</div><div class="ll-vi">'+esc(p.last.vi)+'</div>';
      }
      pollTimer=setTimeout(poll,700);
    }else if(p.status==='done'){
      finish();
    }else if(p.status==='cancelled'){
      resetAfterRun();showErr('Đã huỷ tác vụ.');
    }else{
      resetAfterRun();showErr('Lỗi: '+(p.error||'không rõ'));
    }
  }).catch(function(){
    resetAfterRun();showErr('Mất kết nối tới máy chủ (máy chủ đã tắt?).');
  });
}

function finish(){
  fetch('/api/result?job='+jobId).then(function(r){return r.text()}).then(function(t){
    resultText=t;
    var cues=[];try{cues=JSON.parse(t).cues||[]}catch(e){}
    $('stTotal').textContent=cues.length;
    var ok=0;cues.forEach(function(c){if(c.vi&&c.vi.trim())ok++});
    $('stOk').textContent=ok;
    var bad=cues.length-ok;
    $('stErr').textContent=bad;
    $('stErrWrap').style.display=bad?'':'none';
    var tb=$('tbody');tb.innerHTML='';
    var MAX=400;
    cues.slice(0,MAX).forEach(function(c,i){
      var tr=document.createElement('tr');
      if(!c.vi||!c.vi.trim())tr.className='fail';
      tr.innerHTML='<td class="num">'+(i+1)+'</td><td class="num">'+c.s+'</td><td class="num">'+c.e+
        '</td><td class="ja">'+esc(c.ja)+'</td><td class="vi">'+esc(c.vi||'⚠ chưa dịch được')+'</td>';
      tb.appendChild(tr);
    });
    $('moreNote').textContent=cues.length>MAX?('Hiển thị '+MAX+' / '+cues.length+' câu — bấm “Tải JSON” để có đầy đủ.'):'';
    $('progCard').hidden=true;
    $('doneCard').hidden=false;
    $('doneCard').scrollIntoView({behavior:'smooth'});
    lockUI(false);
  }).catch(function(){
    resetAfterRun();showErr('Không tải được kết quả.');
  });
}

function resetAfterRun(){
  clearTimeout(pollTimer);
  $('progCard').hidden=true;
  lockUI(false);
}

$('btnCancel').addEventListener('click',function(){
  if(jobId)fetch('/api/cancel?job='+jobId,{method:'POST'}).catch(function(){});
});

$('btnDownload').addEventListener('click',function(){
  if(!resultText)return;
  var blob=new Blob([resultText],{type:'application/json;charset=utf-8'});
  var a=document.createElement('a');
  a.href=URL.createObjectURL(blob);a.download=outName;
  document.body.appendChild(a);a.click();a.remove();
  setTimeout(function(){URL.revokeObjectURL(a.href)},5000);
});

function flashCopy(){var m=$('copyMsg');m.hidden=false;setTimeout(function(){m.hidden=true},2000)}

$('btnCopy').addEventListener('click',function(){
  if(!resultText)return;
  if(navigator.clipboard&&navigator.clipboard.writeText){
    navigator.clipboard.writeText(resultText).then(flashCopy,function(){legacyCopy()});
  }else{legacyCopy()}
});

function legacyCopy(){
  var ta=document.createElement('textarea');
  ta.value=resultText;ta.style.position='fixed';ta.style.opacity='0';
  document.body.appendChild(ta);ta.focus();ta.select();
  var ok=false;try{ok=document.execCommand('copy')}catch(e){}
  ta.remove();
  if(ok)flashCopy();else showRaw();
}

function showRaw(){
  if(!resultText)return;
  $('rawTitle').textContent=outName;
  $('rawBox').value=resultText;
  $('rawOverlay').hidden=false;
}
$('btnRaw').addEventListener('click',showRaw);
$('btnRawClose').addEventListener('click',function(){$('rawOverlay').hidden=true});
$('rawOverlay').addEventListener('click',function(e){if(e.target===this)$('rawOverlay').hidden=true});

$('btnAgain').addEventListener('click',function(){
  $('doneCard').hidden=true;$('tbody').innerHTML='';resultText='';jobId=null;
  setFile(null);$('pasteBox').value='';
  window.scrollTo({top:0,behavior:'smooth'});
});
</script>
</body>
</html>
"""


# ===========================================================================
# 9) MÁY CHỦ HTTP
# ===========================================================================

def _get_job(qs):
    jid = (qs.get("job") or [""])[0]
    with _JOBS_LOCK:
        return _JOBS.get(jid)


class Handler(BaseHTTPRequestHandler):
    server_version = "srt2json/" + VERSION
    protocol_version = "HTTP/1.1"

    # ---- tiện ích ----
    def _query(self):
        return urllib.parse.urlsplit(self.path)

    def _qs(self):
        return urllib.parse.parse_qs(self._query().query)

    def _reply(self, code, obj, ctype="application/json; charset=utf-8", extra=None):
        if isinstance(obj, (dict, list)):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        elif isinstance(obj, str):
            body = obj.encode("utf-8")
        else:
            body = obj
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # giữ console sạch

    # ---- GET ----
    def do_GET(self):
        try:
            path = self._query().path
            qs = self._qs()
            if path == "/":
                self._reply(200, PAGE, "text/html; charset=utf-8")
            elif path == "/api/sample":
                self._reply(200, SAMPLE_SRT, "text/plain; charset=utf-8")
            elif path == "/api/progress":
                job = _get_job(qs)
                if not job:
                    self._reply(404, {"error": "không tìm thấy job (có thể đã quá cũ)"})
                    return
                snap = job.snapshot()
                snap["percent"] = round(job.done * 100 / job.total) if job.total else 0
                self._reply(200, snap)
            elif path == "/api/result":
                job = _get_job(qs)
                if not job:
                    self._reply(404, {"error": "không tìm thấy job"})
                    return
                if job.status != "done":
                    self._reply(409, {"error": "chưa hoàn tất (trạng thái: %s)" % job.status})
                    return
                payload = dump_json(job.cues, job.src, job.tgt)
                extra = None
                if qs.get("dl"):
                    fname = os.path.splitext(job.name)[0] + ".json"
                    try:
                        safe = fname.encode("ascii").decode("ascii")
                    except UnicodeEncodeError:
                        safe = "subtitles.json"
                    extra = {"Content-Disposition":
                             "attachment; filename=\"%s\"; filename*=UTF-8''%s"
                             % (safe, urllib.parse.quote(fname))}
                self._reply(200, payload, "application/json; charset=utf-8", extra)
            else:
                self._reply(404, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            try:
                self._reply(500, {"error": str(e)})
            except Exception:
                pass

    # ---- POST ----
    def do_POST(self):
        try:
            path = self._query().path
            qs = self._qs()

            if path == "/api/cancel":
                job = _get_job(qs)
                if job:
                    with job.lock:
                        job.cancel = True
                    self._reply(200, {"ok": True})
                else:
                    self._reply(404, {"ok": False})
                return

            if path != "/api/upload":
                self._reply(404, {"error": "not found"})
                return

            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            if length <= 0:
                self._reply(400, {"error": "Không có dữ liệu."})
                return
            if length > MAX_UPLOAD:
                self._reply(413, {"error": "File quá lớn (tối đa 30 MB)."})
                return
            body = self.rfile.read(length)

            name = os.path.basename((qs.get("name") or ["subtitle.srt"])[0]) or "subtitle.srt"
            keep_tags = (qs.get("tags") or ["0"])[0].lower() in ("1", "true", "yes")
            do_split = (qs.get("split") or ["0"])[0].lower() in ("1", "true", "yes")
            src = (qs.get("src") or ["ja"])[0] or "ja"
            tgt = (qs.get("tgt") or ["vi"])[0] or "vi"
            engine = (qs.get("engine") or ["google"])[0].lower()
            if engine not in ("google", "local"):
                engine = "google"
            try:
                workers = int((qs.get("workers") or ["5"])[0])
            except ValueError:
                workers = 5
            workers = max(1, min(12, workers))

            if engine == "local":
                ok, msg = _local_ready()
                if not ok:
                    self._reply(400, {"error": msg})
                    return

            text = decode_bytes(body)

            # JSON cũ -> chế độ điền nốt câu thiếu
            fix_mode = False
            st = text.lstrip()
            if name.lower().endswith(".json") or st[:1] == "{":
                cues = load_fix_json(text, src, tgt)
                if cues is None:
                    self._reply(400, {"error": "File JSON không đúng định dạng {\"cues\":[...]} "
                                               "(cần có s, e và khóa \"%s\" trong mỗi cue)." % src})
                    return
                fix_mode = True
            else:
                cues = parse_subtitle(text, keep_tags=keep_tags)
                if do_split:
                    cues = split_long_cues(cues)
            if not cues:
                self._reply(400, {"error": "Không tìm thấy dòng timestamp nào — file có phải SRT/VTT không?"})
                return

            job = Job(cues, name, src, tgt, workers=workers, fix=fix_mode,
                      engine=engine)
            with _JOBS_LOCK:
                _cleanup_jobs()
                _JOBS[job.id] = job
            threading.Thread(target=_run_job, args=(job,), daemon=True).start()
            missing = sum(1 for c in cues if not c["tgt"]) if fix_mode else job.total
            self._reply(200, {"job_id": job.id, "total": job.total,
                              "name": job.name, "fix": fix_mode,
                              "missing": missing, "engine": engine})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            try:
                self._reply(500, {"error": str(e)})
            except Exception:
                pass


# ===========================================================================
# 10) CHẾ ĐỘ CLI
# ===========================================================================

def cli_run(args):
    try:
        with open(args.input, "rb") as f:
            raw = f.read()
    except OSError as e:
        sys.exit("Không đọc được file: %s" % e)

    text = decode_bytes(raw)
    err = sys.stderr
    fix_mode = False

    if args.input.lower().endswith(".json") or text.lstrip()[:1] == "{":
        cues = load_fix_json(text, args.src, args.tgt)
        if cues is None:
            sys.exit("File JSON không đúng định dạng {\"cues\":[...]} — kiểm tra lại file đầu vào.")
        fix_mode = True
    else:
        cues = parse_subtitle(text, keep_tags=args.keep_tags)
        if args.split:
            cues = split_long_cues(cues)
    if not cues:
        sys.exit("Không tìm thấy dòng timestamp nào — file có phải SRT/VTT không?")

    if args.engine == "local":
        ok, msg = _local_ready()
        if not ok:
            sys.exit(msg)
        if not _local_model_downloaded():
            print("Lần đầu dùng offline: tự tải model NLLB ~650MB vào thư mục "
                  "models/ (chỉ 1 lần duyệt)…", file=err)

    # thông báo tiến độ -> stderr, để stdout chỉ chứa JSON (tiện pipe)
    _plock = threading.Lock()

    def on_progress(done, total, note=None):
        pct = done * 100 // total if total else 0
        if note == "model":
            tag = "chuẩn bị model offline"
        elif note == "retry":
            tag = "vòng sau, lấy lại câu lỗi"
        else:
            tag = "dịch"
        with _plock:
            err.write("\r  %s: %d / %d (%d%%)" % (tag, done, total, pct))
            err.flush()

    engine_name = "OFFLINE (NLLB)" if args.engine == "local" else \
                  "Google Translate (online, %d luồng)" % args.workers

    if fix_mode:
        missing = [i for i, c in enumerate(cues) if not c["tgt"]]
        print("Chế độ ĐIỀN NỐT: %d/%d câu còn thiếu tiếng Việt."
              % (len(missing), len(cues)), file=err)
        if not missing:
            print("✓ File đã đủ — không cần làm gì.", file=err)
            return
        print("Đang dịch lại %d câu thiếu bằng %s…" % (len(missing), engine_name), file=err)
        try:
            results = translate_texts([cues[i]["src"] for i in missing],
                                      args.src, args.tgt,
                                      on_progress=on_progress,
                                      workers=args.workers,
                                      engine=args.engine,
                                      use_cache=not args.no_cache)
        except KeyboardInterrupt:
            sys.exit("\nĐã huỷ.")
        err.write("\n")
        for k, i in enumerate(missing):
            cues[i]["tgt"] = results[k]
        failed = sum(1 for c in cues if not c["tgt"])
        if failed:
            print("⚠ Vẫn còn %d câu trống — chạy lại lệnh này sau 1-2 phút." % failed, file=err)
    elif not args.no_translate:
        print("Đã đọc %d câu từ %s" % (len(cues), os.path.basename(args.input)), file=err)
        print("Đang dịch %s → %s bằng %s…" % (args.src, args.tgt, engine_name), file=err)
        try:
            results = translate_texts([c["src"] for c in cues], args.src, args.tgt,
                                      on_progress=on_progress,
                                      workers=args.workers,
                                      engine=args.engine,
                                      use_cache=not args.no_cache)
        except KeyboardInterrupt:
            sys.exit("\nĐã huỷ.")
        err.write("\n")
        for c, v in zip(cues, results):
            c["tgt"] = v
        failed = sum(1 for c in cues if not c["tgt"])
        if failed:
            print("⚠ %d câu không dịch được — có thể bị Google chặn tạm. "
                  "Chờ 1-2 phút rồi chạy: python srt2json.py <file-json> để điền nốt, "
                  "hoặc dùng --engine local (offline, không bao giờ bị chặn)." % failed, file=err)
    else:
        print("Đã đọc %d câu (không dịch)." % len(cues), file=err)

    payload = dump_json(cues, args.src, args.tgt)
    if args.output:
        out_path = args.output
    elif fix_mode:
        out_path = args.input  # điền nốt: ghi đè lên chính file đó
    else:
        out_path = None
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
        print("✓ Đã ghi: %s" % out_path, file=err)
    else:
        print(payload)


# ===========================================================================
# 11) CHẾ ĐỘ WEB + main()
# ===========================================================================

def serve(args):
    port = args.port
    server = None
    for _ in range(30):
        try:
            server = ThreadingHTTPServer((args.host, port), Handler)
            break
        except OSError:
            port += 1
    if server is None:
        sys.exit("Không mở được cổng %d–%d. Thử --port khác." % (args.port, args.port + 29))

    display_host = "127.0.0.1" if args.host in ("0.0.0.0", "") else args.host
    url = "http://%s:%d" % (display_host, port)
    print("=" * 56)
    print("  🎬 srt2json v%s — SRT/VTT → JSON · Dịch Nhật → Việt" % VERSION)
    print("=" * 56)
    print("  ➜ Mở trình duyệt: %s" % url)
    if args.host == "0.0.0.0":
        print("    (đang lắng nghe trên mọi giao diện mạng LAN)")
    print("  ➜ Engine: Google (online) · Offline (NLLB) — chọn trong web UI")
    print("  ➜ Tắt máy chủ: nhấn Ctrl+C")
    print()
    if not args.no_browser:
        threading.Timer(0.7, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã tắt máy chủ. Tạm biệt!")
    finally:
        server.server_close()


def main():
    ap = argparse.ArgumentParser(
        prog="srt2json.py",
        description="SRT/VTT → JSON · đọc timestamp · tách từng câu · dịch Nhật → Việt "
                    "(Google Translate online hoặc NLLB offline, miễn phí). "
                    "Chạy KHÔNG có đối số để mở giao diện web. "
                    "Đưa file JSON cũ vào để ĐIỀN NỐT các câu còn thiếu.",
        epilog="Ví dụ:\n"
               "  python srt2json.py                          # mở giao diện web\n"
               "  python srt2json.py video.srt -o out.json    # dịch rồi ghi file\n"
               "  python srt2json.py video.srt --engine local # dịch OFFLINE (NLLB)\n"
               "  python srt2json.py out.json                 # điền nốt câu thiếu\n"
               "  python srt2json.py video.srt --workers 8    # google nhanh hơn\n",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", nargs="?", help="file .srt / .vtt (bỏ trống để chạy web UI); "
                                             "đưa file .json để điền nốt câu thiếu")
    ap.add_argument("-o", "--output", help="file JSON đầu ra (mặc định: in ra màn hình; "
                                           "chế độ điền nốt: ghi đè file vào)")
    ap.add_argument("--engine", choices=("google", "local"), default="google",
                    help="engine dịch: google (online, mặc định) hoặc local "
                         "(offline NLLB, cần: pip install ctranslate2 sentencepiece huggingface_hub)")
    ap.add_argument("--no-cache", action="store_true",
                    help="không dùng/ghi cache dịch trên đĩa")
    ap.add_argument("--no-translate", action="store_true", help="chỉ tách câu, không dịch")
    ap.add_argument("--split", action="store_true",
                    help="tách câu dài thành nhiều câu theo 。！？")
    ap.add_argument("--keep-tags", action="store_true",
                    help="giữ thẻ định dạng <i>, <font>...")
    ap.add_argument("--workers", type=int, default=5,
                    help="số luồng song song cho engine google (mặc định: 5)")
    ap.add_argument("--src", default="ja", help="ngôn ngữ nguồn (mặc định: ja)")
    ap.add_argument("--tgt", default="vi", help="ngôn ngữ đích (mặc định: vi)")
    ap.add_argument("--host", default="127.0.0.1", help="host cho web UI (mặc định: 127.0.0.1)")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT,
                    help="cổng cho web UI (mặc định: %d)" % DEFAULT_PORT)
    ap.add_argument("--no-browser", action="store_true", help="không tự mở trình duyệt")
    args = ap.parse_args()

    if args.input:
        cli_run(args)
    else:
        serve(args)


if __name__ == "__main__":
    main()
