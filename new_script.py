import pandas as pd
import json
import ast

# ===================== ВСПОМОГАТЕЛЬНОЕ =====================


def _safe_parse_json(val):
    """Возвращает dict/list из json_data независимо от формата (str JSON / str python dict / dict)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (dict, list)):
        return val
    assert isinstance(val, str), "json_data должен быть str или dict/list"
    try:
        return json.loads(val)
    except json.JSONDecodeError:
        # часто строки с одинарными кавычками — это питоновский литерал
        return ast.literal_eval(val)


def _union(intervals):
    """Слияние пересекающихся/смежных интервалов [(start,end|NaT)] → список без пересечений."""
    if not intervals:
        return []
    df = pd.DataFrame(intervals, columns=['start', 'end']).sort_values('start')
    df['end_filled'] = df['end'].fillna(pd.Timestamp.max)
    merged, cs, ce = [], df.iloc[0]['start'], df.iloc[0]['end_filled']
    for i in range(1, len(df)):
        s, e = df.iloc[i]['start'], df.iloc[i]['end_filled']
        if s <= ce:
            ce = max(ce, e)
        else:
            merged.append((cs, ce))
            cs, ce = s, e
    merged.append((cs, ce))
    return [(s, (pd.NaT if e == pd.Timestamp.max else e)) for s, e in merged]


def _subtract(A, B):
    """A \ B для объединений интервалов. На выходе непокрытые куски A."""
    def fill(x): return x if pd.notna(x) else pd.Timestamp.max
    A = [(a, fill(b)) for a, b in _union(A)]
    B = [(a, fill(b)) for a, b in _union(B)]
    if not A:
        return []
    if not B:
        return [(a, (pd.NaT if b == pd.Timestamp.max else b)) for a, b in A]
    res = []
    for a0, a1 in A:
        cur = a0
        for b0, b1 in B:
            if b1 <= cur or b0 >= a1:
                continue
            if b0 > cur:
                res.append((cur, b0))
            cur = max(cur, b1)
            if cur >= a1:
                break
        if cur < a1:
            res.append((cur, a1))
    return [(s, (pd.NaT if e == pd.Timestamp.max else e)) for s, e in res if (e - s).total_seconds() > 0]

# ===================== 1) ИНТЕРВАЛЫ ИП ИЗ DaData =====================


def parse_dadata_intervals(json_val_or_str):
    """
    Возвращает интервалы работы ИП [(start, end|NaT), ...] из одного json_data.
    Используй поверх всех строк по одному ИНН и потом _union().
    """
    data = _safe_parse_json(json_val_or_str)
    rows = []
    for s in (data or {}).get('suggestions', []):
        st = (s.get('data') or {}).get('state') or {}
        reg = st.get('registration_date')
        if reg is None:
            continue
        liq = st.get('liquidation_date')
        start = pd.to_datetime(reg, unit='ms', utc=True).tz_convert(None)
        end = (pd.to_datetime(liq, unit='ms', utc=True).tz_convert(None)
               if liq is not None else pd.NaT)
        rows.append((start, end))
    return _union(rows)


def dadata_intervals_for_inn(final_df, inn_value, json_col='json_data'):
    """Собирает интервалы ИП по всем строкам данного ИНН в final_df."""
    intervals = []
    for js in final_df.loc[final_df['inn'] == inn_value, json_col]:
        intervals += parse_dadata_intervals(js)
    return _union(intervals)


def gaps_without_ip(ip_open_intervals):
    """Разрывы между интервалами ИП (когда ИП закрыт): [(gap_start, gap_end)]."""
    if not ip_open_intervals:
        return []
    filled = [(s, (e if pd.notna(e) else pd.Timestamp.max))
              for s, e in ip_open_intervals]
    gaps = []
    for (s1, e1), (s2, e2) in zip(filled[:-1], filled[1:]):
        if e1 < s2:
            gaps.append((e1, s2))
    return [(a, (pd.NaT if b == pd.Timestamp.max else b)) for a, b in gaps if (b - a).total_seconds() > 0]

# ===================== 2) ИНТЕРВАЛЫ ВРЕМЕННОГО ЗАКРЫТИЯ ДЛЯ ТОЧКИ =====================


def temp_closed_intervals_for_point(df_point, closed_col='date_temp_closed', reopen_col='date_reopen'):
    """
    По строкам одной точки (одного id_cd) строит интервалы temp_closed → reopen.
    Поддерживает несколько пар, незакрытый интервал тянется до +∞ (NaT).
    """
    g = df_point.copy()
    g[closed_col] = pd.to_datetime(g[closed_col], errors='coerce')
    g[reopen_col] = pd.to_datetime(g[reopen_col], errors='coerce')

    events = []
    for _, r in g.iterrows():
        if pd.notna(r[closed_col]):
            events.append(('open',  r[closed_col]))
        if pd.notna(r[reopen_col]):
            events.append(('close', r[reopen_col]))
    if not events:
        return []

    events.sort(key=lambda x: (x[1], 0 if x[0] == 'open' else 1))
    intervals, stack = [], None
    for typ, ts in events:
        if typ == 'open':
            if stack is None:
                stack = ts
            else:
                # второе подряд "open" — считаем закрытием предыдущего в момент нового
                intervals.append((stack, ts))
                stack = ts
        else:  # close
            if stack is not None and ts >= stack:
                intervals.append((stack, ts))
                stack = None
    if stack is not None:  # незакрытый тянется до +∞
        intervals.append((stack, pd.NaT))
    return _union(intervals)

# ===================== 3) ОСНОВНАЯ: ПРОВЕРКА ПО КАЖДОЙ ТОЧКЕ (id_cd) =====================


def flag_points_for_inn(final_df, df, inn_value,
                        point_col='id_cd',
                        closed_col='date_temp_closed',
                        reopen_col='date_reopen',
                        grace_days=0):
    """
    По одному ИНН:
      - строит интервалы ИП (DaData) и "гэпы без ИП";
      - для КАЖДОЙ точки (point_col=id_cd) строит интервалы temp_closed;
      - учитывает фактическую дату открытия точки (open_date);
      - проверяет покрытие: каждую "дыру без ИП" должна закрывать точка.

    Возвращает (flags_df, ip_open_intervals, ip_gaps).
    flags_df колонки:
      inn, id_cd, ok_flag, gap_start, gap_end, uncovered_start, uncovered_end,
      closed_dates, reopen_dates, tc_intervals, point_open_date
    """
    # интервалы ИП из DaData
    ip_open = dadata_intervals_for_inn(final_df, inn_value)
    # исходные "гэпы без ИП" (между периодами действия ИП)
    gaps = gaps_without_ip(ip_open)

    # все строки большой таблицы по этому ИНН
    df_inn = df.loc[df['inn'] == inn_value].copy()

    rows = []
    for point, df_point in df_inn.groupby(point_col, dropna=False):
        # списки дат закрытий/открытий точки (для наглядности), без заглушек 1970-01-01
        _cl = pd.to_datetime(df_point[closed_col], errors='coerce')
        _ro = pd.to_datetime(df_point[reopen_col], errors='coerce')
        mask1970_cl = _cl.dt.normalize().eq(pd.Timestamp('1970-01-01'))
        mask1970_ro = _ro.dt.normalize().eq(pd.Timestamp('1970-01-01'))
        closed_list = _cl[~mask1970_cl].dropna().sort_values().tolist()
        reopen_list = _ro[~mask1970_ro].dropna().sort_values().tolist()

        # фактическая дата открытия точки (минимальная), без заглушки 1970-01-01
        _po = pd.to_datetime(df_point.get('open_date'), errors='coerce')
        if isinstance(_po, pd.Series):
            mask1970_po = _po.dt.normalize().eq(pd.Timestamp('1970-01-01'))
            _po = _po[~mask1970_po]
            point_open = _po.min()
        else:
            point_open = pd.NaT

        # интервалы временного закрытия точки
        tc = temp_closed_intervals_for_point(
            df_point, closed_col=closed_col, reopen_col=reopen_col)
        if grace_days:
            tc = [(s, (e + pd.Timedelta(days=grace_days) if pd.notna(e) else e))
                  for s, e in tc]

        # фильтруем "гэпы без ИП" под жизнь точки (только пересекающиеся с периодом после её открытия)
        point_gaps = gaps
        if pd.notna(point_open):
            point_gaps = [(max(gs, point_open), ge)
                          for gs, ge in gaps if (pd.isna(ge) or ge > point_open)]
            point_gaps = [(gs, ge)
                          for gs, ge in point_gaps if (pd.isna(ge) or ge > gs)]

        any_violation = False
        if point_gaps:
            for gs, ge in point_gaps:
                # непокрытая часть gap интервалами закрытий точки
                uncovered = _subtract([(gs, ge)], tc)
                if uncovered:
                    any_violation = True
                    for us, ue in uncovered:
                        rows.append({
                            'inn': inn_value,
                            point_col: point,
                            'ok_flag': False,
                            'gap_start': gs, 'gap_end': ge,
                            'uncovered_start': us, 'uncovered_end': ue,
                            'closed_dates': closed_list,
                            'reopen_dates': reopen_list,
                            'tc_intervals': tc,
                            'point_open_date': point_open
                        })

        # если гэпов не было или все покрыты — положительная запись
        if not point_gaps or not any_violation:
            rows.append({
                'inn': inn_value,
                point_col: point,
                'ok_flag': True,
                'gap_start': None, 'gap_end': None,
                'uncovered_start': None, 'uncovered_end': None,
                'closed_dates': closed_list,
                'reopen_dates': reopen_list,
                'tc_intervals': tc,
                'point_open_date': point_open
            })

    return pd.DataFrame(rows), ip_open, gaps

# ===================== 4) ЗАПУСК ПО ВСЕМ ИНН =====================


def run_all(final_df, df,
            point_col='id_cd',
            closed_col='date_temp_closed',
            reopen_col='date_reopen',
            grace_days=0):
    out = []
    for inn_value in final_df['inn'].dropna().unique():
        flags_df, _, _ = flag_points_for_inn(final_df, df, inn_value,
                                             point_col=point_col,
                                             closed_col=closed_col,
                                             reopen_col=reopen_col,
                                             grace_days=grace_days)
        out.append(flags_df)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
        columns=['inn', point_col, 'ok_flag', 'gap_start',
                 'gap_end', 'uncovered_start', 'uncovered_end']
    )
