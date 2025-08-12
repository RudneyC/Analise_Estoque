# app_abastecimento.py
# -*- coding: utf-8 -*-
"""
📦 Painel de Sugestão de Abastecimento por Loja
Sem abreviar nomes de colunas. Mantém todas as funcionalidades.
Otimizado para desempenho com st.dataframe e vectorized Pandas.
"""

import io, re, sys, traceback
import numpy as np
import pandas as pd
import streamlit as st

# --------------- Config
st.set_page_config(page_title="📦 Sugestão de Abastecimento por Loja", layout="wide")
st.title("📦 Painel de Sugestão de Abastecimento por Loja")

# --------- Formatadores (pt-BR)
def fmt_qtd(x): return "" if pd.isna(x) else f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
def fmt_rs(x):  return "" if pd.isna(x) else "R$ " + f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
def fmt_mes(x): return "" if pd.isna(x) else f"{x:,.1f}".replace(".", ",")
def fmt_pct(x): return "" if pd.isna(x) else f"{x*100:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")

# --------- Colunas esperadas a partir de "Dimensão Planilha"
DIM_SPLIT_COLS = ["Filial", "Codigo_Produto", "Descricao_Produto", "Locacao", "Linha", "Sublinha"]

# --------- Possíveis colunas numéricas no Excel
NUM_HINTS = {
    "MED (02-04)", "DISPONIVEL$QSUM|SALDOS", "NORMAL$QSUM|BO",
    "_Quantidade Ideal", "_A Pedir", "_Quantidade Excesso",
    "_Valor a Comprar", "_Valor Excesso",
    "DEM (1-1)", "VND (01-01)", "VR. DISP. EST$SUM|PRECO",
    "Meses Disponiveis", "produto_margem", "produto_valor_publico", "produto_valor_venda_gestor",
    "FREQ (02-13)|VENDAS", "DEM (2-4)", "%DEM (02-04)", "QTD_IDEAL$Q|SALDOS",
    "_Normal+UnParada", "_CustoMedioDisp", "VND (02-04)", "VLR_ORC_02_04",
    "RequisitadosQtd", "MRG UNIT (02-04)", "FAT (02-04)", "VND 8-13", "SKU"
}

# ---------------- Helpers de ingestão/conversão
def to_number(val):
    if pd.isna(val): return pd.NA
    s = str(val).strip().replace("R$", "").replace("\xa0", "").replace(" ", "")
    if s == "" or s.lower() in {"nan", "none", "nat"}: return pd.NA
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s: s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-+eE]", "", s)
    try: return float(s)
    except: return pd.NA

def safe_num_col(df, col):
    if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
        df[col] = df[col].map(to_number)

def read_excel_file(file) -> pd.DataFrame:
    try:
        return pd.read_excel(file, engine="openpyxl")
    except ImportError:
        st.error("Instale **openpyxl**: `pip install -U openpyxl`.")
        st.stop()

def split_dimensao(df: pd.DataFrame) -> pd.DataFrame:
    if "Dimensão Planilha" not in df.columns:
        st.error("A planilha precisa da coluna **'Dimensão Planilha'**.")
        st.stop()
    parts = df["Dimensão Planilha"].astype(str).str.split(r"\s*\|\|\s*", expand=True)
    if parts.shape[1] < 6:
        st.error("Esperado: Filial || Código || Descrição || Locação || Linha || Sublinha.")
        st.stop()
    parts = parts.iloc[:, :6]
    parts.columns = DIM_SPLIT_COLS
    for c in parts.columns: parts[c] = parts[c].astype(str).str.strip()
    return pd.concat([df, parts], axis=1)

def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in (NUM_HINTS & set(df.columns)): safe_num_col(df, c)
    for c in df.select_dtypes(include="object").columns:
        if c in DIM_SPLIT_COLS or c == "Dimensão Planilha": continue
        sample = df[c].dropna().astype(str).head(200)
        if len(sample) and sample.str.contains(r"[0-9]").mean() >= 0.8:
            safe_num_col(df, c)
    return df

def criticidade_from_meses(x):
    try:
        v = float(x)
        if v < 1: return "Ruptura"
        if v < 2: return "Atenção"
        return "OK"
    except: return "—"

def add_sugestao(df: pd.DataFrame) -> pd.DataFrame:
    ap = df.get("_A Pedir", 0)
    ex = df.get("_Quantidade Excesso", 0)
    def decide(a, e):
        a = 0 if pd.isna(a) else float(a)
        e = 0 if pd.isna(e) else float(e)
        if a > 0: return "Comprar"
        if e > 0: return "Transferir"
        return "OK"
    df["Sugestao_Acao"] = [decide(a, e) for a, e in zip(ap, ex)]
    return df

# ---------------- Upload & utilidades
uploaded_file = st.file_uploader("📁 Envie o arquivo Excel com a planilha:", type="xlsx")

with st.sidebar:
    st.header("🧰 Utilidades")
    if st.button("🧹 Limpar cache"):
        st.cache_data.clear()
        st.rerun()

if not uploaded_file:
    st.info("👈 Envie a planilha para iniciar.")
    st.stop()

# ---------------- Pipeline
try:
    df = read_excel_file(uploaded_file)
    df = split_dimensao(df)
    df = convert_numeric_columns(df)
    df["Criticidade"] = df.get("Meses Disponiveis", np.nan).map(criticidade_from_meses)
    df = add_sugestao(df)
except Exception:
    st.error("Falha ao processar arquivo.")
    st.code("".join(traceback.format_exception(*sys.exc_info())))
    st.stop()

# ---------------- Filtros
st.sidebar.header("🎛️ Filtros")
filiais = sorted(df["Filial"].dropna().unique().tolist()) if "Filial" in df.columns else []
linhas = sorted(df["Linha"].dropna().unique().tolist()) if "Linha" in df.columns else []
sugs = sorted(df["Sugestao_Acao"].dropna().unique().tolist()) if "Sugestao_Acao" in df.columns else []

filial_sel = st.sidebar.multiselect("Filial", filiais, default=filiais)
linha_sel = st.sidebar.multiselect("Linha", linhas, default=linhas)
sug_sel = st.sidebar.multiselect("Sugestao_Acao", sugs, default=sugs)

df_filt = df.copy()
if filial_sel: df_filt = df_filt[df_filt["Filial"].isin(filial_sel)]
if linha_sel: df_filt = df_filt[df_filt["Linha"].isin(linha_sel)]
if sug_sel: df_filt = df_filt[df_filt["Sugestao_Acao"].isin(sug_sel)]
st.caption(f"Linhas após filtros: **{len(df_filt):,}**")

# ================= KPIs "fortes" (estilo Tableau) =================
st.subheader("📈 Indicadores Gerais")

def _get(col):  # busca coluna se existir
    return df_filt[col] if col in df_filt.columns else pd.Series(dtype=float)

# Quantidades
qtd_disp = _get("DISPONIVEL$QSUM|SALDOS").fillna(0).astype(float)
qtd_bo = _get("NORMAL$QSUM|BO").fillna(0).astype(float)
qtd_req = _get("RequisitadosQtd").fillna(0).astype(float) if "RequisitadosQtd" in df_filt.columns else qtd_bo

# Valores
vr_disp_est = _get("VR. DISP. EST$SUM|PRECO").fillna(0).astype(float)
fat_024 = _get("FAT (02-04)").fillna(0).astype(float)
cus_024 = _get("VLR_ORC_02_04").fillna(0).astype(float)

# “A Pedir” e “Excesso” (somente positivos)
a_pedir_qtd = _get("_A Pedir").fillna(0).clip(lower=0)
excesso_qtd = _get("_Quantidade Excesso").fillna(0).clip(lower=0)
a_comprar = _get("_Valor a Comprar").fillna(0).clip(lower=0)
v_excesso = _get("_Valor Excesso").fillna(0).clip(lower=0)

# KPIs principais
kpi_estoque_total_qtd = (qtd_disp + qtd_bo).sum() or 0
kpi_disponivel_qtd = qtd_disp.sum() or 0
kpi_requisitados_qtd = qtd_req.sum() or 0
kpi_pedidos_fornec = qtd_bo.sum() or 0

kpi_custo_medio_estoque = (vr_disp_est.sum() / (qtd_disp.sum() or 1))
kpi_custo_medio_req = ((qtd_req * (vr_disp_est / (qtd_disp.replace(0, np.nan)))).fillna(0).sum() / (qtd_req.sum() or 1))

kpi_valor_estoque = vr_disp_est.sum()
kpi_valor_req = (qtd_req * (vr_disp_est / (qtd_disp.replace(0, np.nan)))).fillna(0).sum()
kpi_valor_dispon = kpi_valor_estoque

kpi_meses_dispon = _get("Meses Disponiveis").replace([np.inf, -np.inf], np.nan).dropna().mean() or 0

# Pedido (dias) ≈ soma(A_Pedir) / soma(demanda diária)
med_024 = _get("MED (02-04)").fillna(0).astype(float)
dem_dia = (med_024 / 30.0)
kpi_pedido_dias = (a_pedir_qtd.sum() / (dem_dia.sum() + 1e-9))

kpi_skus = int(df_filt[["Filial", "Codigo_Produto"]].drop_duplicates().shape[0]) if {"Filial", "Codigo_Produto"}.issubset(df_filt.columns) else df_filt.shape[0]

# Mostradores com cores estilo Tableau
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Estoque Total (Qtd)", fmt_qtd(kpi_estoque_total_qtd), delta_color="off")
c2.metric("Custo Médio (R$)", fmt_rs(kpi_custo_medio_estoque), delta_color="off")
c3.metric("Valor Estoque Total (R$)", fmt_rs(kpi_valor_estoque), delta_color="off")
c4.metric("Estoque Total (Meses)", fmt_mes(kpi_meses_dispon), delta_color="off")
c5.metric("Requisitados (Qtd)", fmt_qtd(kpi_requisitados_qtd), delta_color="off")

c6, c7, c8, c9, c10 = st.columns(5)
c6.metric("Disponível (Qtd)", fmt_qtd(kpi_disponivel_qtd), delta_color="off")
c7.metric("Custo Médio Req. (R$)", fmt_rs(kpi_custo_medio_req), delta_color="off")
c8.metric("Valor Requisitado (R$)", fmt_rs(kpi_valor_req), delta_color="off")
c9.metric("Disponível (Meses)", fmt_mes(kpi_meses_dispon), delta_color="off")
c10.metric("SKU (Qtd)", fmt_qtd(kpi_skus), delta_color="off")

# Barras (Faturamento/CMV/Margem mensais)
st.markdown(" ")
col_a, col_b, col_c = st.columns(3)
mens_fat = (fat_024.sum() / 3.0)
mens_cmv = (cus_024.sum() / 3.0)
mens_mrg = mens_fat - mens_cmv
with col_a:
    st.metric("Faturamento Mensal (R$)", fmt_rs(mens_fat), delta_color="off")
with col_b:
    st.metric("CMV Mensal (R$)", fmt_rs(mens_cmv), delta_color="off")
with col_c:
    st.metric("Margem Mensal (R$)", fmt_rs(mens_mrg), delta_color="off")

# ---------------- KPIs por Filial
st.markdown("#### 📊 KPIs por Filial")
if "Filial" in df_filt.columns:
    kpi_df = (
        df_filt.assign(
            _AP=df_filt.get("_A Pedir", 0).clip(lower=0),
            _QE=df_filt.get("_Quantidade Excesso", 0).clip(lower=0),
            _VC=df_filt.get("_Valor a Comprar", 0).clip(lower=0),
            _VE=df_filt.get("_Valor Excesso", 0).clip(lower=0),
        )
        .groupby("Filial")[["_AP", "_QE", "_VC", "_VE", "Meses Disponiveis"]]
        .agg({"_AP": "sum", "_QE": "sum", "_VC": "sum", "_VE": "sum", "Meses Disponiveis": "mean"})
        .rename(columns={
            "_AP": "_A Pedir (Qtd)",
            "_QE": "_Quantidade Excesso (Qtd)",
            "_VC": "_Valor a Comprar (R$)",
            "_VE": "_Valor Excesso (R$)",
            "Meses Disponiveis": "Meses_Estoque_Médio"
        })
        .reset_index()
    )
    st.dataframe(
        kpi_df,
        use_container_width=True,
        height=260,
        column_config={
            "_A Pedir (Qtd)": st.column_config.NumberColumn(format="%,.0f"),
            "_Quantidade Excesso (Qtd)": st.column_config.NumberColumn(format="%,.0f"),
            "_Valor a Comprar (R$)": st.column_config.NumberColumn(format="R$ %,.2f"),
            "_Valor Excesso (R$)": st.column_config.NumberColumn(format="R$ %,.2f"),
            "Meses_Estoque_Médio": st.column_config.NumberColumn(format="%.1f"),
        }
    )

# ---------------- Detalhamento dos Itens
st.subheader("🧾 Detalhamento dos Itens (ordenado por prioridade)")

# Ordenação: maior A_Pedir primeiro; se empatar, menor meses disponível
ord_cols = [c for c in ["_A Pedir", "Meses Disponiveis"] if c in df_filt.columns]
ascending = [False, True][:len(ord_cols)]
df_view = df_filt.sort_values(by=ord_cols, ascending=ascending)

# Seleção de colunas úteis primeiro
cols_first = [c for c in [
    "Filial", "Codigo_Produto", "Descricao_Produto", "Linha", "Sublinha", "Locacao",
    "Sugestao_Acao", "Criticidade",
    "_A Pedir", "_Quantidade Excesso", "_Valor a Comprar", "_Valor Excesso",
    "Meses Disponiveis", "produto_margem",
    "DISPONIVEL$QSUM|SALDOS", "NORMAL$QSUM|BO", "MED (02-04)", "FAT (02-04)", "VR. DISP. EST$SUM|PRECO"
] if c in df_view.columns]
df_view = df_view[cols_first + [c for c in df_view.columns if c not in cols_first]]

# Limite de linhas renderizadas
MAX_SHOW = st.sidebar.number_input("Máximo de linhas exibidas", 1000, 20000, 8000, 1000)
st.dataframe(
    df_view.head(MAX_SHOW),
    use_container_width=True,
    height=420,
    column_config={
        "_A Pedir": st.column_config.NumberColumn(format="%,.0f"),
        "_Quantidade Excesso": st.column_config.NumberColumn(format="%,.0f"),
        "_Valor a Comprar": st.column_config.NumberColumn(format="R$ %,.2f"),
        "_Valor Excesso": st.column_config.NumberColumn(format="R$ %,.2f"),
        "Meses Disponiveis": st.column_config.NumberColumn(format="%.1f"),
        "produto_margem": st.column_config.NumberColumn(format="%.1f%%"),
        "DISPONIVEL$QSUM|SALDOS": st.column_config.NumberColumn(format="%,.0f"),
        "NORMAL$QSUM|BO": st.column_config.NumberColumn(format="%,.0f"),
        "MED (02-04)": st.column_config.NumberColumn(format="%,.0f"),
        "FAT (02-04)": st.column_config.NumberColumn(format="%,.0f"),
        "VR. DISP. EST$SUM|PRECO": st.column_config.NumberColumn(format="R$ %,.2f"),
        "Sugestao_Acao": st.column_config.TextColumn(
            "Sugestão de Ação",
            help="Ação recomendada: Comprar, Transferir ou OK"
        ),
        "Criticidade": st.column_config.TextColumn(
            "Criticidade",
            help="Nível de urgência: Ruptura (<1 mês), Atenção (<2 meses), OK"
        )
    }
)
st.caption(f"Exibindo {min(MAX_SHOW, len(df_view)):,} de {len(df_view):,} linhas. Para desempenho, a estilização foi desativada.")

# ---------------- Sugestão de Transferências
# ---------------- Sugestão de Transferências (origem → destino)
st.markdown("### 🔁 Sugestão de Transferências (origem → destino)")

# Parâmetro: manter X meses de cobertura na ORIGEM (com base em MED (02-04))
meses_minimos = st.sidebar.number_input(
    "Cobertura mínima na origem (meses)", min_value=0.0, value=1.5, step=0.5
)

req_cols = {
    "Filial","Codigo_Produto","_A Pedir","_Quantidade Excesso",
    "DISPONIVEL$QSUM|SALDOS","NORMAL$QSUM|BO","MED (02-04)"
}
if req_cols.issubset(df_filt.columns):
    base = (
        df_filt[["Filial","Codigo_Produto","_A Pedir","_Quantidade Excesso",
                 "DISPONIVEL$QSUM|SALDOS","NORMAL$QSUM|BO","MED (02-04)"]]
        .rename(columns={
            "_Quantidade Excesso":"Excesso",
            "DISPONIVEL$QSUM|SALDOS":"Disp",
            "NORMAL$QSUM|BO":"BO",
            "MED (02-04)":"MED"
        })
        .copy()
    )
    for c in ["_A Pedir","Excesso","Disp","BO","MED"]:
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0).clip(lower=0)

    transfers = []
    plan_future = []
    MIN_Q = 1  # não existe quantidade menor que 1

    import math
    for prod, sub in base.groupby("Codigo_Produto"):
        # 1) Consolida por filial
        agg = (
            sub.groupby("Filial", as_index=False)[["_A Pedir","Excesso","Disp","BO","MED"]].sum()
              .assign(
                  # 2) Liquidação intra-filial (compensação local)
                  need_raw=lambda d: (d["_A Pedir"] - d["Excesso"]).clip(lower=0.0),
                  excesso_liq=lambda d: (d["Excesso"] - d["_A Pedir"]).clip(lower=0.0),
                  # 3) Estoque mínimo (qtd) = MED * meses_minimos
                  estoque_min_qtd=lambda d: d["MED"] * float(meses_minimos),
              )
        )

        # 4) Limite transferível AGORA (sem contar BO) e quantização para inteiro
        agg = agg.assign(
            transferivel_agora_raw=lambda d: np.maximum(
                np.minimum(d["excesso_liq"], np.maximum(d["Disp"] - d["estoque_min_qtd"], 0.0)),
                0.0
            ),
        )
        # Inteirizar necessidades e sobras
        agg["need"] = np.ceil(agg["need_raw"]).astype(int)                 # destino sempre pede inteiro
        agg["transferivel_agora"] = np.floor(agg["transferivel_agora_raw"]).astype(int)  # origem só transfere inteiros
        agg["transferivel_apos_bo"] = np.maximum(
            agg["excesso_liq"] - agg["transferivel_agora"], 0
        ).astype(int)  # informativo (inteiro)

        # Listas para alocação imediata (somente >= 1)
        falta = agg.loc[agg["need"] >= MIN_Q, ["Filial","need"]].sort_values("need", ascending=False).values.tolist()
        sobra = agg.loc[agg["transferivel_agora"] >= MIN_Q, ["Filial","transferivel_agora"]].sort_values("transferivel_agora", ascending=False).values.tolist()

        # Planejamento (excedente que só poderá sair após BO)
        fut = agg.loc[agg["transferivel_apos_bo"] >= MIN_Q, ["Filial","transferivel_apos_bo"]]
        if not fut.empty:
            for _, r in fut.iterrows():
                plan_future.append((prod, r["Filial"], int(r["transferivel_apos_bo"])))

        # 5) Alocação gulosa com salvaguarda Origem ≠ Destino (sempre inteiros)
        i = j = 0
        while i < len(falta) and j < len(sobra):
            fil_dest, need = falta[i]
            fil_orig, have = sobra[j]

            if fil_orig == fil_dest:
                # não deve ocorrer após compensação; mantém segurança
                if need <= have: i += 1
                else: j += 1
                continue

            q = min(int(need), int(have))  # inteiro
            if q >= MIN_Q:
                transfers.append((prod, fil_orig, fil_dest, q))
                need -= q
                have -= q

            falta[i][1] = need
            sobra[j][1] = have
            if need < MIN_Q: i += 1
            if have < MIN_Q: j += 1

    if transfers:
        transf_df = pd.DataFrame(transfers, columns=["Codigo_Produto","Origem","Destino","Qtd_Transferir"]).astype({"Qtd_Transferir":"int64"})

        # Custo médio da ORIGEM (se disponível)
        if {"VR. DISP. EST$SUM|PRECO", "DISPONIVEL$QSUM|SALDOS"}.issubset(df_filt.columns):
            custo_med = (
                df_filt.groupby(["Filial","Codigo_Produto"])
                .apply(lambda g: (g["VR. DISP. EST$SUM|PRECO"].sum() /
                                  (g["DISPONIVEL$QSUM|SALDOS"].sum() or 1)),
                       include_groups=False)
                .reset_index().rename(columns={0:"Custo_Medio"})
            )
            transf_df = (
                transf_df.merge(
                    custo_med.rename(columns={"Filial":"Origem"}),
                    on=["Origem","Codigo_Produto"], how="left"
                )
                .assign(
                    Valor_Transferir=lambda d: d["Custo_Medio"].fillna(0) * d["Qtd_Transferir"],
                    De_Para=lambda d: d["Origem"].astype(str) + " \u2192 " + d["Destino"].astype(str)
                )
                .sort_values(["Origem","Destino","Valor_Transferir"], ascending=[True,True,False])
            )

        st.dataframe(
            transf_df, use_container_width=True, height=330,
            column_config={
                "De_Para": st.column_config.TextColumn("de: → para:"),
                "Qtd_Transferir": st.column_config.NumberColumn(format="%,.0f"),
                "Custo_Medio": st.column_config.NumberColumn(format="R$ %,.2f"),
                "Valor_Transferir": st.column_config.NumberColumn(format="R$ %,.2f"),
            }
        )

        resumo = transf_df.groupby(["Origem","Destino"], as_index=False).agg(
            Qtd_Total=("Qtd_Transferir","sum"),
            Valor_Total=("Valor_Transferir","sum")
        ).sort_values("Valor_Total", ascending=False)
        st.markdown("**Resumo por par Origem → Destino (imediato)**")
        st.dataframe(
            resumo, use_container_width=True, height=240,
            column_config={
                "Qtd_Total": st.column_config.NumberColumn(format="%,.0f"),
                "Valor_Total": st.column_config.NumberColumn(format="R$ %,.2f"),
            }
        )
    else:
        st.info("Sem pares com sobra e falta para sugerir transferência imediata.")

    # Planejamento: só após receber BO (mostra inteiros)
    if plan_future:
        fut_df = (
            pd.DataFrame(plan_future, columns=["Codigo_Produto","Filial_Origem","Qtd_Transferivel_Apos_BO"])
              .groupby(["Codigo_Produto","Filial_Origem"], as_index=False)["Qtd_Transferivel_Apos_BO"].sum()
              .astype({"Qtd_Transferivel_Apos_BO":"int64"})
        )
        with st.expander("📦 Planejamento (aguardando BO)"):
            st.dataframe(
                fut_df, use_container_width=True, height=220,
                column_config={"Qtd_Transferivel_Apos_BO": st.column_config.NumberColumn(format="%,.0f")}
            )
else:
    st.warning("Para calcular transferências por meses de cobertura, faltam colunas obrigatórias.")





# ---------------- Exportação
st.subheader("📤 Exportar dados filtrados (sem abreviar colunas)")
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
    df_view.to_excel(writer, index=False, sheet_name="Sugestao_Abastecimento")
    if "Filial" in df_filt.columns:
        kpi_df.to_excel(writer, index=False, sheet_name="KPIs_Filial")
    if "transf_df" in locals() and not transf_df.empty:
        transf_df.to_excel(writer, index=False, sheet_name="Transferencias")

    wb = writer.book
    f_qtd = wb.add_format({"num_format": "#,##0"})
    f_rs = wb.add_format({"num_format": "R$ #,##0.00"})
    f_mes = wb.add_format({"num_format": "0.0"})
    f_pct = wb.add_format({"num_format": "0.0%"})

    def fmt_sheet(ws, headers):
        for j, h in enumerate(headers):
            if h in ["_A Pedir", "_Quantidade Excesso", "DISPONIVEL$QSUM|SALDOS", "NORMAL$QSUM|BO", "VND (02-04)", "FAT (02-04)", "Qtd_Transferir"]:
                ws.set_column(j, j, 14, f_qtd)
            elif h in ["_Valor a Comprar", "_Valor Excesso", "VR. DISP. EST$SUM|PRECO", "Valor_Transferir", "Custo_Medio"]:
                ws.set_column(j, j, 16, f_rs)
            elif h in ["Meses Disponiveis"]:
                ws.set_column(j, j, 10, f_mes)
            elif h in ["produto_margem", "%DEM (02-04)"]:
                ws.set_column(j, j, 10, f_pct)
            else:
                ws.set_column(j, j, 22)
        ws.freeze_panes(1, 0)

    fmt_sheet(writer.sheets["Sugestao_Abastecimento"], df_view.columns.tolist())
    if "KPIs_Filial" in writer.sheets:
        fmt_sheet(writer.sheets["KPIs_Filial"], kpi_df.columns.tolist())
    if "transf_df" in locals() and "Transferencias" in writer.sheets:
        fmt_sheet(writer.sheets["Transferencias"], transf_df.columns.tolist())

st.download_button(
    "📥 Baixar Excel (Sugestão + KPIs + Transferências)",
    buffer.getvalue(),
    file_name="sugestao_abastecimento.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
