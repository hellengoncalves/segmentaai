import cx_Oracle
from statsmodels.tsa.seasonal import seasonal_decompose
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt 
import seaborn as sns 
from sklearn.preprocessing import scale
from sklearn.covariance import EmpiricalCovariance, MinCovDet
from scipy import stats as st 
import pingouin as pg
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, PowerTransformer
from sklearn.impute import SimpleImputer
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score

username = 'XXXXXXXXXXXXX'
password = 'XXXXXXXXXXXXX'
dsn = 'XXXXXXXXXXXXXXXXXXXX' 
connection = cx_Oracle.connect(username, password, dsn)

#Tabela de Clientes
query_tb_clientes = "SELECT * FROM tb_clientes"
df_tb_clientes = pd.read_sql(query_tb_clientes, con=connection)

#Tabela de Contratos
query_tb_contratos = "SELECT * FROM tb_contratos"
df_tb_contratos = pd.read_sql(query_tb_contratos, con=connection)

#Tabela de Transações
query_tb_transacoes = "SELECT * FROM tb_transacoes"
df_tb_transacoes = pd.read_sql(query_tb_transacoes, con=connection)

#Tabela de Tickets Suporte
query_tb_ticket = "SELECT * FROM tb_ticket"
df_tb_ticket = pd.read_sql(query_tb_ticket, con=connection)

#1. Missings
#Realizado para cada DF, mas deixamos um default
missings_por_coluna = df_tb_transacoes.isna().sum()

missings_por_coluna = pd.DataFrame(missings_por_coluna, 
                                   columns=['n']) \
                                   .reset_index() \
                                   .rename(columns={'index': 'variaveis'}
                                   )

missings_por_coluna

#2. Outliers
df_tb_transacoes['vl_total'] = pd.to_numeric(df_tb_transacoes['vl_total'].str.replace(',', '.'), errors='coerce')
df_tb_transacoes['valor_z'] = scale(df_tb_transacoes['vl_total']).astype(float)
df_tb_transacoes

df_tb_contratos['valor_total_contrato'] = pd.to_numeric(df_tb_contratos['valor_total_contrato'].str.replace(',', '.'), errors='coerce')
df_tb_contratos['valor_z'] = scale(df_tb_contratos['valor_total_contrato']).astype(float)
df_tb_contratos

#Verificar a normalização do dado, com média próxima de 0 e desvio próximo de 1
df_tb_transacoes[['vl_total', 'valor_z']].describe()

df_tb_contratos[['valor_total_contrato', 'valor_z']].describe()

#Tratamento
wins_values = df_tb_contratos['valor_total_contrato'].quantile([0.05, 0.90]).to_list()

df_tb_contratos['valor_wins'] = df_tb_contratos['valor_total_contrato'].clip(wins_values[0], wins_values[1])

df_tb_contratos[['valor_total_contrato', 'valor_wins']].boxplot()

RANDOM_STATE = 42

#1. Limpezas e Transformações
def _to_datetime(series, dayfirst=True):
    """Converte para datetime com coerce e dayfirst=True por padrão."""
    return pd.to_datetime(series, errors='coerce', dayfirst=dayfirst)

def _to_numeric(series):
    """Converte strings monetárias para float (coerce)."""
    s = series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(s, errors='coerce')

#2. Construção das features
def construir_features(
    df_tb_clientes: pd.DataFrame,
    df_tb_contratos: pd.DataFrame,
    df_tb_ticket: pd.DataFrame,  
    col_id='cd_cliente',
    data_base: pd.Timestamp | None = None
) -> pd.DataFrame:
    dfc = df_tb_clientes.copy()
    dft = df_tb_contratos.copy()
    dftk = df_tb_ticket.copy()  

    # --- normalizações existentes ---
    dfc['dt_abertura'] = _to_datetime(dfc['dt_abertura'])
    dft['data_assinatura_contrato'] = _to_datetime(dft['data_assinatura_contrato'])
    dftk['dt_criacao'] = _to_datetime(dftk['dt_criacao'])  

    if not np.issubdtype(dft['valor_total_contrato'].dtype, np.number):
        dft['valor_total_contrato'] = _to_numeric(dft['valor_total_contrato'])

    # Periodicidade → meses
    period_map = {
        '00 - Mensal': 1,
        '01 - Bimestral': 2,
        '02 - Trimestral': 3,
        '03 - Quadrimestral': 4,
        '05 - Semestral': 6,
        '11 - Anual': 12,
        '-': np.nan
    }
    dft['period_meses'] = dft['periodicidade'].map(period_map)

    # --- data_base ---
    if data_base is None:
        candidates = [pd.Timestamp.today().normalize()]
        if dft['data_assinatura_contrato'].notna().any():
            candidates.append(dft['data_assinatura_contrato'].max().normalize())
        if dfc['dt_abertura'].notna().any():
            candidates.append(dfc['dt_abertura'].max().normalize())
        if dftk['dt_criacao'].notna().any():
            candidates.append(dftk['dt_criacao'].max().normalize()) 
        data_base = max(candidates)

    # --- janela 12 meses contratos ---
    inicio_12m = data_base - relativedelta(months=12)
    contratos_12m = dft[
        (dft['data_assinatura_contrato'] >= inicio_12m) &
        (dft['data_assinatura_contrato'] <= data_base)
    ]

    # --- agregações 12m contratos ---
    agg_12m = contratos_12m.groupby(col_id).agg(
        qt_contratos_12m=('data_assinatura_contrato', 'count'),
        valor_contratado_12m=('valor_total_contrato', 'sum'),
        produtos_distintos_12m=('produto', pd.Series.nunique),
        periodicidade_max_meses_12m=('period_meses', 'max')
    ).reset_index()
    agg_12m['fechou_>=1ano_12m'] = agg_12m['periodicidade_max_meses_12m'].fillna(0) >= 12
    agg_12m['multi_produto_12m'] = agg_12m['produtos_distintos_12m'].fillna(0) > 1

    # --- agregações lifetime ---
    agg_total = dft.groupby(col_id).agg(
        produtos_distintos_total=('produto', pd.Series.nunique),
        periodicidade_max_meses_total=('period_meses', 'max')
    ).reset_index()
    agg_total['fechou_>=1ano_total'] = agg_total['periodicidade_max_meses_total'].fillna(0) >= 12
    agg_total['multi_produto_total'] = agg_total['produtos_distintos_total'].fillna(0) > 1

    # --- base clientes + tempo de vida ---
    base = dfc[[col_id, 'dt_abertura', 'vl_faixa_faturamento']].copy()
    base['tempo_vida_dias'] = (data_base - base['dt_abertura']).dt.days
    base['tempo_vida_anos'] = base['tempo_vida_dias'] / 365.25

    # --- nova variável: média de tickets por mês ---
    tickets_12m = dftk[
        (dftk['dt_criacao'] >= inicio_12m) &
        (dftk['dt_criacao'] <= data_base)
    ]
    agg_tickets = tickets_12m.groupby(col_id).agg(
        qt_tickets_12m=('dt_criacao', 'count')
    ).reset_index()
    # calcular média por mês (12 meses)
    agg_tickets['ticket_medio_por_mes'] = agg_tickets['qt_tickets_12m'] / 12

    # --- mergear tudo ---
    feats = (base
             .merge(agg_12m, on=col_id, how='left')
             .merge(agg_total[[col_id, 'fechou_>=1ano_total', 'multi_produto_total']], on=col_id, how='left')
             .merge(agg_tickets[[col_id, 'ticket_medio_por_mes']], on=col_id, how='left'))  

    # --- preenchimentos ---
    fill_zero = ['qt_contratos_12m', 'valor_contratado_12m', 'produtos_distintos_12m', 'periodicidade_max_meses_12m', 'ticket_medio_por_mes']
    for c in fill_zero:
        if c in feats.columns:
            feats[c] = feats[c].fillna(0)

    fill_false = ['fechou_>=1ano_12m','multi_produto_12m','fechou_>=1ano_total','multi_produto_total']
    for c in fill_false:
        if c in feats.columns:
            feats[c] = feats[c].fillna(False)

    feats = feats[feats['tempo_vida_dias'].notna()].copy()
    feats['data_base'] = data_base

    return feats

#3. Pré-Processamento e Transformações
def treinar_clusters(feats: pd.DataFrame, col_id='cd_cliente'):
    numeric_features = [
        'tempo_vida_anos',
        'qt_contratos_12m',
        'valor_contratado_12m',
        'produtos_distintos_12m',
        'periodicidade_max_meses_12m',
        'fechou_>=1ano_12m',
        'multi_produto_total'
    ]

    for c in ['fechou_>=1ano_12m', 'multi_produto_total']:
        if feats[c].dtype == bool:
            feats[c] = feats[c].astype(int)

    categorical_features = ['vl_faixa_faturamento']
    X = feats[numeric_features + categorical_features].copy()

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', Pipeline(steps=[
                ('imp', SimpleImputer(strategy='median')),
                ('pow', PowerTransformer(method='yeo-johnson')), 
                ('sc', StandardScaler())
            ]), numeric_features),
            ('cat', Pipeline(steps=[
                ('imp', SimpleImputer(strategy='most_frequent')),
                ('oh', OneHotEncoder(handle_unknown='ignore'))
            ]), categorical_features)
        ],
        remainder='drop'
    )

    Z = preprocessor.fit_transform(X)

    resultados_kmeans = []
    melhor = {'k': None, 'sil': -1, 'db': np.inf, 'ch': -1, 'modelo': None, 'labels': None}
    for k in range(2, 10):
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init='auto')
        labels = km.fit_predict(Z)
        sil = silhouette_score(Z, labels)
        db = davies_bouldin_score(Z, labels)
        ch = calinski_harabasz_score(Z, labels)
        resultados_kmeans.append({'k': k, 'silhouette': sil, 'davies_bouldin': db, 'calinski_harabasz': ch})
        if sil > melhor['sil'] or (sil == melhor['sil'] and db < melhor['db']):
            melhor.update({'k': k, 'sil': sil, 'db': db, 'ch': ch, 'modelo': km, 'labels': labels})

    resultados_gmm = []
    melhor_gmm = {'k': None, 'bic': np.inf, 'modelo': None, 'labels': None}
    for k in range(2, 10):
        gmm = GaussianMixture(n_components=k, covariance_type='full', random_state=RANDOM_STATE)
        gmm.fit(Z)
        bic = gmm.bic(Z)
        resultados_gmm.append({'k': k, 'bic': bic})
        if bic < melhor_gmm['bic']:
            melhor_gmm.update({'k': k, 'bic': bic, 'modelo': gmm, 'labels': gmm.predict(Z)})

    usar_gmm = melhor['sil'] < 0.20
    final_model = melhor_gmm['modelo'] if usar_gmm else melhor['modelo']
    final_labels = melhor_gmm['labels'] if usar_gmm else melhor['labels']
    final_alg = 'GMM' if usar_gmm else 'KMeans'
    final_k = melhor_gmm['k'] if usar_gmm else melhor['k']

    saida = feats[[col_id]].copy()
    saida['cluster'] = final_labels

    perfil = (feats
          .join(saida.set_index(col_id), on=col_id)
          .groupby('cluster')
          .agg(
              clientes=('cluster', 'size'),
              tempo_vida_anos_median=('tempo_vida_anos', 'median'),
              qt_contratos_12m_median=('qt_contratos_12m', 'median'),
              valor_contratado_12m_median=('valor_contratado_12m', 'median'),
              produtos_distintos_12m_median=('produtos_distintos_12m', 'median'),
              periodicidade_max_meses_12m_max=('periodicidade_max_meses_12m', 'max'),
              **{'pct_fechou_>=1ano_12m': ('fechou_>=1ano_12m', lambda s: 100 * s.mean())},
              **{'pct_multi_produto_total': ('multi_produto_total', lambda s: 100 * s.mean())}
          )
          .sort_values('clientes', ascending=False)
          .reset_index())

    return {
        'preprocessor': preprocessor,
        'algoritmo': final_alg,
        'k': final_k,
        'modelo': final_model,
        'labels': final_labels,
        'perfil_clusters': perfil,
        'metricas_kmeans': pd.DataFrame(resultados_kmeans),
        'metricas_gmm': pd.DataFrame(resultados_gmm)
    }

#4. Análise do resultado (exportando)
def exportar_resultados(feats, resultado, col_id='cd_cliente',
                        nome_base='clientes_clusterizados'):
    df_final = feats.copy()
    df_final['cluster'] = resultado['labels']

    df_final.to_csv(f"{nome_base}.csv", sep=';', index=False, encoding='utf-8')
    df_final.to_excel(f"{nome_base}.xlsx", index=False)
    resultado['perfil_clusters'].to_csv(f"{nome_base}_perfil.csv", sep=';', index=False, encoding='utf-8')
    resultado['perfil_clusters'].to_excel(f"{nome_base}_perfil.xlsx", index=False)

    print("Arquivos gerados:")
    print(f" - {nome_base}.csv")
    print(f" - {nome_base}.xlsx")
    print(f" - {nome_base}_perfil.csv")
    print(f" - {nome_base}_perfil.xlsx")

    return df_final

def plot_clusters_pca(feats, resultado, col_id='cd_cliente'):
    numeric_features = [
        'tempo_vida_anos',
        'qt_contratos_12m',
        'valor_contratado_12m',
        'produtos_distintos_12m',
        'periodicidade_max_meses_12m',
        'fechou_>=1ano_12m',
        'multi_produto_total'
    ]
    categorical_features = ['vl_faixa_faturamento']

    X = feats[numeric_features + categorical_features].copy()
    for c in ['fechou_>=1ano_12m', 'multi_produto_total']:
        if X[c].dtype == bool:
            X[c] = X[c].astype(int)

    Z = resultado['preprocessor'].transform(X)

    pca = PCA(n_components=2, random_state=42)
    Z2D = pca.fit_transform(Z)

    labels = resultado['labels']
    alg = resultado['algoritmo']
    k = resultado['k']

    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(Z2D[:, 0], Z2D[:, 1], c=labels, cmap='tab10', s=60, alpha=0.7, edgecolors='k')
    plt.title(f"Clusters de Clientes ({alg}, K={k}) - PCA 2D", fontsize=14)
    plt.xlabel("PCA 1")
    plt.ylabel("PCA 2")
    handles, _ = scatter.legend_elements()
    plt.legend(handles, [f"Cluster {i}" for i in range(k)], title="Clusters")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.show()

df_tb_clientes.columns = df_tb_clientes.columns.str.lower()
df_tb_contratos.columns = df_tb_contratos.columns.str.lower()
df_tb_ticket.columns = df_tb_ticket.columns.str.lower()

#dataset de features
feats = construir_features(df_tb_clientes, df_tb_contratos, df_tb_ticket, col_id='cd_cliente')

#clusterização
resultado = treinar_clusters(feats, col_id='cd_cliente')

df_final = exportar_resultados(feats, resultado, col_id='cd_cliente', nome_base='clientes_clusterizados')

plot_clusters_pca(feats, resultado, col_id='cd_cliente')