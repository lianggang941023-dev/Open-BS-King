# -*- coding: utf-8 -*-
"""「上一次拿到的好数据」—— 单文件覆盖式存档（v2.399）

用户 m08249 (2026-11-27) 原话:
    能不能把龙虎榜和连板梯队也做成本地覆盖性存档, 拉到新数据就覆盖存档文件,
    确保文件不会无限变多占用磁盘空间, 只有一个文件, 拉不到新数据就用旧数据,
    这样你可以把缓存和旧数据拿去暂时盯上, 界面上能看到数据时间就行

背景:
    同花顺 fuyao 的「涨跌停池 / 龙虎榜」这一组后端(hotspot_focus)会整块挂掉,
    返回 code=5003「数据源不可用」。挂了以后我们原来直接抛异常 / 返回空表,
    界面就变成「今日无连板数据」「龙虎榜不可用」—— 看着像"今天真没有", 其实拿不到。

设计(三条硬约束, 对照用户原话):
  ① **每个数据集只有一个文件** —— `<name>.json` 原地覆盖, 文件名里不带日期,
     绝不按天累积。所以 data_archive/lastgood/ 里永远只有固定那几个文件。
  ② **只有更好的数据才覆盖** —— `save()` 里三条闸:
       · payload 必须非空(`count`/`total` > 0)
       · 数据日不能比存档里那份更旧(防"上游回退到上一交易日"把新数据盖成旧的)
       · 内容哈希没变就跳过(免得每 30 秒重写一次盘)
  ③ **自带时间戳** —— `data_date` = 这份数据是哪天的行情, `saved_at` = 我们什么时候拿到的。
     界面把两个都显示出来, 用户一眼能看出"这是旧数据, 停在几号几点"。

对外:
    save(name, payload, data_date='', force=False) -> 写入路径 / ''(跳过)
    load(name)                                     -> {'ok', 'name', 'data_date', 'saved_at',
                                                       'age_sec', 'payload'} / {'ok': False, 'error'}
    info()                                         -> 只看这个目录有多大(排查用)
"""
import hashlib
import io
import json
import os
import tempfile
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARC_DIR = os.path.join(BASE_DIR, 'data_archive')
LG_DIR = os.path.join(ARC_DIR, 'lastgood')

LG_FMT = 1

_LAST_HASH = {}


def _ensure():
    try:
        os.makedirs(LG_DIR, exist_ok=True)
    except Exception:
        pass
    return os.path.isdir(LG_DIR)


def _path(name):
    safe = ''.join(c if (c.isalnum() or c in '_-') else '_' for c in str(name or 'x'))
    return os.path.join(LG_DIR, safe + '.json')


def _stamp():
    return time.strftime('%Y-%m-%d %H:%M:%S')


def _atomic_write(path, text):
    """先写同目录临时文件再 os.replace —— 进程被杀也不会留半截 JSON"""
    d = os.path.dirname(path)
    _ensure()
    fd, tmp = tempfile.mkstemp(dir=d, prefix='.tmp_', suffix='.json')
    try:
        with io.open(fd, 'w', encoding='utf-8', newline='\n') as f:
            f.write(text)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def _size_of(payload):
    """这份 payload 「有多少条」—— 池子看 total, 龙虎榜看 count, 都没有就看是不是非空"""
    if not isinstance(payload, dict):
        return 0
    for k in ('total', 'count', 'n'):
        try:
            v = int(payload.get(k) or 0)
            if v > 0:
                return v
        except Exception:
            pass
    # v2.399: hot_money 也要算 —— 游资榜的 rows 是空的、内容全在 hot_money 分组里,
    #   只看 rows/tiers 会把它当成空表, 存档永远写不进去(实测 lhb_hm 一直缺档)。
    if payload.get('rows') or payload.get('tiers') or payload.get('stock_items'):
        return 1
    if payload.get('hot_money'):
        return 1
    return 0


def save(name, payload, data_date='', force=False):
    """把这次拿到的好数据覆盖进单文件存档。返回写入路径, 跳过/失败返回 ''。"""
    try:
        if not _ensure():
            return ''
        if not isinstance(payload, dict) or not payload:
            return ''
        # ① 空表绝不覆盖 —— 空表本身就是"没拿到", 存进去等于把好数据抹掉
        if _size_of(payload) <= 0:
            return ''
        raw_date = str(data_date or payload.get('date') or '').replace('-', '')[:8]
        # ② 更旧的数据不许覆盖更新的存档
        if not force and raw_date and raw_date.isdigit() and len(raw_date) == 8:
            old = _read_meta(name)
            if old:
                od = str(old.get('data_date') or '').replace('-', '')[:8]
                if od and od.isdigit() and od > raw_date:
                    return ''
        # ③ 内容没变就别重写盘
        try:
            h = hashlib.md5(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                       separators=(',', ':')).encode('utf-8', 'replace')).hexdigest()
        except Exception:
            h = ''
        if not force and h and _LAST_HASH.get(name) == h:
            return ''
        obj = {
            'fmt': LG_FMT,
            'kind': 'lastgood',
            'name': name,
            'data_date': ('%s-%s-%s' % (raw_date[:4], raw_date[4:6], raw_date[6:8])
                          if (len(raw_date) == 8 and raw_date.isdigit()) else str(data_date or '')),
            'saved_at': _stamp(),
            'saved_ts': time.time(),
            'count': _size_of(payload),
            'payload': payload,
        }
        _atomic_write(_path(name), json.dumps(obj, ensure_ascii=False, separators=(',', ':')))
        if h:
            _LAST_HASH[name] = h
        return _path(name)
    except Exception:
        return ''


def _read_meta(name):
    try:
        p = _path(name)
        if not os.path.isfile(p):
            return None
        with io.open(p, encoding='utf-8') as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def load(name):
    """读单文件存档。**不联网、不重算** —— 只读盘, 快到前端点开就能画。"""
    try:
        d = _read_meta(name)
        if not d or 'payload' not in d:
            return {'ok': False, 'name': name, 'error': '还没有存档'}
        age = 0.0
        try:
            age = max(0.0, time.time() - float(d.get('saved_ts') or 0))
        except Exception:
            age = 0.0
        return {
            'ok': True,
            'name': name,
            'data_date': d.get('data_date') or '',
            'saved_at': d.get('saved_at') or '',
            'age_sec': round(age, 1),
            'count': d.get('count') or 0,
            'payload': d.get('payload'),
        }
    except Exception as e:
        return {'ok': False, 'name': name, 'error': str(e)[:120]}


def age_of(iso):
    '''「某个 YYYY-MM-DD HH:MM:SS 时刻」到现在多少秒; 解析不了返回 None（v2.399）'''
    try:
        s = str(iso or '').strip()
        if len(s) < 19:
            return None
        tt = time.mktime(time.strptime(s[:19], '%Y-%m-%d %H:%M:%S'))
        return max(0.0, time.time() - tt)
    except Exception:
        return None


def drop(name):
    """删掉某个存档（用户想彻底重置某块数据时用）"""
    try:
        p = _path(name)
        if os.path.isfile(p):
            os.unlink(p)
        _LAST_HASH.pop(name, None)
        return True
    except Exception:
        return False


def info():
    """这个目录现在有几个文件、总共多大 —— 验证「不会无限变多」用"""
    out = {'dir': LG_DIR, 'names': [], 'files': 0, 'bytes': 0}
    try:
        for fn in sorted(os.listdir(LG_DIR)):
            if not fn.endswith('.json') or fn.startswith('.tmp_'):
                continue
            p = os.path.join(LG_DIR, fn)
            try:
                st = os.stat(p)
            except Exception:
                continue
            out['files'] += 1
            out['bytes'] += st.st_size
            out['names'].append({'name': fn[:-5], 'size': st.st_size,
                                 'mtime': time.strftime('%Y-%m-%d %H:%M:%S',
                                                        time.localtime(st.st_mtime))})
    except Exception:
        pass
    return out


if __name__ == '__main__':
    print(json.dumps(info(), ensure_ascii=False, indent=2))
