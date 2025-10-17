df_postings_filtered = df_postings_filtered.loc[:, ~df_postings_filtered.columns.duplicated()].copy()
df_postings_filtered["current_user_id"] = df_postings_filtered["current_user_id"].astype(str)

item_col = "item_id_str" if "item_id_str" in df_postings_filtered.columns else ("ItemId" if "ItemId" in df_postings_filtered.columns else None)
if item_col is None:
    raise KeyError("Не найдена колонка товара в df_postings_filtered (ожидались 'item_id_str' или 'ItemId').")
df_postings_filtered[item_col] = df_postings_filtered[item_col].astype(str)

# === 1) Готовим таблицу связей пользователей (affiliate + self) ===
mapping_aff = mapping.copy()
mapping_aff["relation"] = "affiliate"
mapping_aff = mapping_aff.rename(columns={"duplicate_user_id": "candidate_user_id"})[
    ["original_user_id", "candidate_user_id", "relation"]
]

# self-связи (на случай, если понадобится контроль или отчёт)
original_ids_all = sorted(set(mapping["original_user_id"].astype(str)))  # можно дополнить из df_base при желании
mapping_self = pd.DataFrame({
    "original_user_id": original_ids_all,
    "candidate_user_id": original_ids_all,
    "relation": "self",
})

df_candidates = pd.concat([mapping_aff, mapping_self], ignore_index=True).drop_duplicates()
df_candidates["original_user_id"] = df_candidates["original_user_id"].astype(str)
df_candidates["candidate_user_id"] = df_candidates["candidate_user_id"].astype(str)

# === 2) Пары из base: (original_user_id, ItemIdBefore) + ключи заказа из base для контроля ===
have_base_keys = [c for c in ["ClientPostingID", "PostingNumber"] if c in df_base.columns]
df_base_pairs = (
    df_base[["user_id", "ItemIdBefore"] + have_base_keys]
    .dropna(subset=["user_id", "ItemIdBefore"])
    .astype(str)
    .drop_duplicates()
    .rename(columns={"user_id": "original_user_id"})
)

# === 3) join1: кто сделал возврат (affiliate/self) ===
tmp = df_postings_filtered.merge(
    df_candidates,
    left_on="current_user_id",
    right_on="candidate_user_id",
    how="inner"
)
# tmp: у каждой строки возврата теперь есть original_user_id и relation (affiliate/self)

# === 4) join2: матч товара (ItemIdBefore == ItemId) в разрезе original_user_id ===
df_matches = tmp.merge(
    df_base_pairs,
    left_on=["original_user_id", item_col],         # товар из возврата
    right_on=["original_user_id", "ItemIdBefore"],  # товар из base
    how="inner",
    suffixes=("", "_base")
)

# === 5) Убираем self-кейсы «тот же заказ», чтобы не считать его повторно ===
cols = set(df_matches.columns)
same_by_clientposting = False
if {"ClientPostingId", "ClientPostingID_base"} <= cols:
    same_by_clientposting = df_matches["ClientPostingId"].astype(str) == df_matches["ClientPostingID_base"].astype(str)

same_by_postingnumber = False
if {"PostingNumber", "PostingNumber_base"} <= cols:
    same_by_postingnumber = df_matches["PostingNumber"].astype(str) == df_matches["PostingNumber_base"].astype(str)

if isinstance(same_by_clientposting, (pd.Series, np.ndarray)) or isinstance(same_by_postingnumber, (pd.Series, np.ndarray)):
    same_order = (
        (same_by_clientposting if isinstance(same_by_clientposting, (pd.Series, np.ndarray)) else False) |
        (same_by_postingnumber if isinstance(same_by_postingnumber, (pd.Series, np.ndarray)) else False)
    )
    mask_same_self = (df_matches["relation"] == "self") & same_order
    df_matches = df_matches[~mask_same_self].copy()

# === 6) Разделяем на affiliate / self (если нужно анализировать отдельно) ===
df_aff_returns  = df_matches[df_matches["relation"] == "affiliate"].copy()
df_self_returns = df_matches[df_matches["relation"] == "self"].copy()  # опционально

# === 7) Немного метрик и удобный просмотр ===
log(f"Все совпадения после фильтров: {len(df_matches)} | из них affiliate: {len(df_aff_returns)} | self: {len(df_self_returns)}")

pref_cols = [
    "original_user_id", "relation", "candidate_user_id",
    "ItemIdBefore", item_col, "PostingIsReturn",
    "PostingNumber_base", "ClientPostingID_base",   # из base
    "PostingNumber", "ClientPostingId",             # из возврата
    "OrderDate", "Price", "FirstPrice", "RezonItemId", "current_user_id"
]
view_cols = [c for c in pref_cols if c in df_aff_returns.columns]
display(df_aff_returns[view_cols].head(10) if view_cols else df_aff_returns.head(10))
