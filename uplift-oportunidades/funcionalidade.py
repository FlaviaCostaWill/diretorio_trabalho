import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import boto3
import io
from io import StringIO
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Table, MetaData
import gspread
from google.oauth2 import service_account
from scipy import stats
from datetime import datetime
from dateutil.relativedelta import relativedelta

def analise_campanhas(usuario_athena, dt_ini, dt_fim, chave, df):
#df = arquivo da campanha com colunas cpf e grupo (alvo e controle)
    
    import warnings
    warnings.filterwarnings('ignore')
    import sys
    sys.tracebacklimit=0
    
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario_athena
    SCHEMA = usuario_athena
    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
    conn_str = con1 + con2
    engine_athena = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
        region_name="sa-east-1",
        schema_name=SCHEMA,
        s3_staging_dir=quote_plus(STAGING_DIR)))
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    try:
        df_cv = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    
    sys.tracebacklimit=None
    
    df_cv['cpf'] = df_cv['cpf'].astype('string').str.zfill(11)
    
    if chave == 'cpf':
        df['cpf'] = df['cpf'].astype('string').str.zfill(11)
    else:
        pass
        
    df = df.join(df_cv.set_index(chave), how = 'left', on = chave)
    #tratar os nulls da flag converteu, spending e transações
    df['converteu'] = df['converteu'].fillna(0.0)
    df['spending_corr'] = df['spending'].fillna(0.0)
    df['transacoes_corr'] = df['transacoes'].fillna(0.0)
    
    #agregado com media por grupo alvo e controle avg(converteu), avg(spending), avg(frequencia)
    #print dos resultados dos testes de diferença de grupos: conversão, spending, frequencia
      
    agg = df.groupby('grupo').agg({'grupo':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    #lift do resultado das tabelas acima

    print("Lift da conversão: "     + str(round(((agg.iloc[0,1] / agg.iloc[1,1]) -1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['converteu'], df.query('grupo == "controle"')['converteu'])
    print("Estatística t do teste de diferença de proporções: "+str(round(t,2)))
    print("P-valor do teste de diferença de proporções: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de conversão não é significativa")
    else:
        print("A diferença de conversão é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift do spending: "     + str(round(((agg.iloc[0,2] / agg.iloc[1,2]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['spending_corr'], df.query('grupo == "controle"')['spending_corr'])
    print("Estatística t do teste de spending: "+str(round(t,2)))
    print("P-valor do teste de spending: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de spending não é significativa")
    else:
        print("A diferença de spending é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift da frequência: "     + str(round(((agg.iloc[0,4] / agg.iloc[1,4]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['transacoes_corr'], df.query('grupo == "controle"')['transacoes_corr'])
    print("Estatística t do teste de frequencia: "+str(round(t,2)))
    print("P-valor do teste de frequencia: "+str(round(p,6)))
    if p >0.05:
        print("A diferença de frequência não é significativa")
    else:
        print("A diferença de frequência é estatisticamente significativa")
    print("______________________________________________________________")
    
    #criar faixa de frequencia e spending
    def fx_freq(x):
        if x == 1:
            return 'a. 1 compra'
        elif x == 2:
            return 'b. 2 compras'
        elif x == 3:
            return 'c. 3 compras'
        elif x == 4:
            return 'd. 4 compras'
        elif x == 5:
            return 'e. 5 compras'
        elif x > 5:
            return 'f. Mais de 5 compras'   
        else:
            return 'g. Sem compras'
    
    def fx_spend(x):
        if x < 300:
            return 'a. até R$ 300'
        elif x >= 300 and x < 600:
            return 'b. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'c. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'd. até R$ 1500'
        elif x >= 1500:
            return 'e. mais de R$ 1500'
        else:
            return 'f. Sem compras'
        
    def fx_lim(x):
        if x < 100:
            return 'a. até R$ 100'
        elif x >= 100 and x < 300:
            return 'b. até R$ 300'
        elif x >= 300 and x < 600:
            return 'c. até R$ 600'
        elif x >= 600 and x < 1000:
            return 'd. até R$ 1000'
        elif x >= 1000 and x < 1500:
            return 'e. até R$ 1500'
        elif x >= 1500:
            return 'f. mais de R$ 1500'
        else:
            return 'g. Sem info'
        
    
    df['fx_frequencia'] = df['transacoes'].apply(fx_freq)
    df['fx_spending'] = df['spending'].apply(fx_spend)
       
    
    del df_cv
    
    #consulta com características anteriores a campanha (ultimos x dias): recencia, fx_frequencia, fx_spending
    ini = datetime.strptime(dt_ini, '%Y-%m-%d')
    ini_antes = (ini + relativedelta(days=-15)).strftime('%Y-%m-%d')

    query = """
                    With uso_credito_pag as (		
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    min(vl_limite_disp) as vl_limite_disp,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                   , base_union as (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    approx_percentile(vl_limite_disp,0.5) as limite_disp_antes,
                    sum(transacoes) as transacoes_antes,
                    sum(spending) as spending_antes
                    from base_union
                   where dia >= '"""
    
    query_final = query + ini_antes + "' and dia < '" + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    df = df.join(df_antes[[chave, 'transacoes_antes', 'spending_antes', 'limite_disp_antes']].set_index(chave), how = 'left', on = chave)
    
    df['fx_frequencia_antes'] = df['transacoes_antes'].apply(fx_freq)
    df['fx_spending_antes'] = df['spending_antes'].apply(fx_spend)
    df['fx_limite'] = df['limite_disp_antes'].apply(fx_lim)
    
    
    print("___________________________________________")
    print("Distribuição de Frequência Antes x Depois")
    print(df['fx_frequencia_antes'].value_counts(normalize = True))
    print(df['fx_frequencia'].value_counts(normalize = True))
    
    print("___________________________________________")
    print("Distribuição de Spending Antes x Depois")
    print(df['fx_spending_antes'].value_counts(normalize = True))
    print(df['fx_spending'].value_counts(normalize = True))    
    
    
    #conversão por faixa anterior de frequencia e spending
    
    print("___________________________________________")
    print("conversão por faixa anterior de frequencia e spending")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_frequencia_antes').agg({'fx_frequencia_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_spending_antes').agg({'fx_spending_antes':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    print("conversão por limite disponível")
    
    agg = df[df['grupo'] == 'alvo'].groupby('fx_limite').agg({'fx_limite':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    
    #consulta recência
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    max(dia) as ultima_compra
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia < '"""
    
    query_final = query + dt_ini + "' group by 1"
    sys.tracebacklimit=0
    df_antes = pd.read_sql(query_final, engine_athena)
    sys.tracebacklimit=None
    df_antes['cpf'] = df_antes['cpf'].astype('string').str.zfill(11)
    
    df_antes['dt_ref'] = datetime.strptime(dt_ini, '%Y-%m-%d')
    df_antes['ultima_compra']  = pd.to_datetime(df_antes['ultima_compra'] ,format='%Y-%m-%d')
       
    df_antes['Dif'] = (df_antes['dt_ref'] - df_antes['ultima_compra']).dt.days
    
    def faixas_dias(x):
        if x < 30:
            return 'a. menos 1 mes'
        elif x < 60:
            return 'b. 1 mes'
        elif x < 90:
            return 'c. 2 meses'
        elif x < 120:
            return 'd. 3 meses'
        elif x < 149:
            return 'e. 4 meses'
        elif x < 179:
            return 'f. 5 meses'
        elif x < 209:
            return 'g. 6 meses' 
        elif x < 239:
            return 'h. 7 meses'
        elif x < 269:
            return 'i. 8 meses'
        elif x < 299:
            return 'j. 9 meses' 
        elif x < 329:
            return 'k. 10 meses' 
        elif x < 364:
            return 'l. 11 meses'
        elif x < 7200:
            return 'm. mais de 12 meses'  
        else:
            return 'n. sem transacao'

    df = df.join(df_antes[[chave, 'Dif']].set_index(chave), how = 'left', on = chave)
    
    df['fx_recencia'] = df['Dif'].apply(faixas_dias)
    print("conversão por faixa de recência")
    agg = df[df['grupo'] == 'alvo'].groupby('fx_recencia').agg({'fx_recencia':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    
    
    
    
def traz_conversao(usuario_athena, dt_ini, dt_fim, chave, df):
    
    import warnings
    warnings.filterwarnings('ignore')
    import sys
    sys.tracebacklimit=0
    
    ACCESS_KEY_ID_WILL = os.getenv('AWS_ACCESS_KEY_ID_WILL')
    SECRET_ACCESS_KEY_WILL = os.getenv('AWS_SECRET_ACCESS_KEY_WILL')
    STAGING_DIR = 's3://data-athena-query-result-will-prod/' + usuario_athena
    SCHEMA = usuario_athena
    con1 = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/"
    con2 = "{schema_name}?s3_staging_dir={s3_staging_dir}"
    conn_str = con1 + con2
    engine_athena = create_engine(conn_str.format(
        aws_access_key_id=quote_plus(ACCESS_KEY_ID_WILL),
        aws_secret_access_key=quote_plus(SECRET_ACCESS_KEY_WILL),
        region_name="sa-east-1",
        schema_name=SCHEMA,
        s3_staging_dir=quote_plus(STAGING_DIR)))
    
    query = """
                    With uso_credito_pag as (			
                    --gera spending pag			
                    select			
                    nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'pag' as origem_trans,
					count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_pag		
                    where	is_saque = false	
                    and	is_cancelada = false		
                    and	is_recarga = false		
                    and	is_pag_limite_utilizado = 'C'
                    and ds_customer = 'pag'
                    group by 1, 2, 3	
                    )
                    , uso_credito_will as (			
                    --gera spending will			
                    select	nr_cpf, id_pag_account as conta_cartao,
                    substring(cast(dt_autorizacao as varchar), 1, 10)  as dia,	
                    'will' as origem_trans,
                    count(distinct id_transaction) as transacoes,
                    sum(vl_real) as spending			
                    from	platform_curated_zone.authorization_will
                    where   ds_transacao = 'credito'			
                    and	ds_status_compra = 'aprovada'		
                    and	cd_retorno = '00'
                    and ds_customer = 'will'
                    and ds_mcc != 'TRANSACOES WEBSERVICE'
                    group by 1, 2, 3		
                    )
                    select 
                    nr_cpf as cpf, 
                    max(conta_cartao) as conta_cartao,
                    1 as converteu,
                    sum(transacoes) as transacoes,
                    sum(spending) as spending
                    from (
                    select  *			
                    from  uso_credito_pag			
                    union all			
                    select  *			
                    from  uso_credito_will)
                   where dia >= '"""
    
    query_final = query + dt_ini + "' and dia <= '" + dt_fim + "' group by 1"
    try:
        df_cv = pd.read_sql(query_final, engine_athena)
    except HTMLParseError:
        pass
    
    sys.tracebacklimit=None
    
    df_cv['cpf'] = df_cv['cpf'].astype('string').str.zfill(11)
    
    if chave == 'cpf':
        df['cpf'] = df['cpf'].astype('string').str.zfill(11)
    else:
        pass
        
    df = df.join(df_cv.set_index(chave), how = 'left', on = chave)
    #tratar os nulls da flag converteu, spending e transações
    df['converteu'] = df['converteu'].fillna(0.0)
    df['spending_corr'] = df['spending'].fillna(0.0)
    df['transacoes_corr'] = df['transacoes'].fillna(0.0)
    return df
    
    
def testes_iterativos(df):
    #df já contendo a conversão: campos converteu, spending e transacoes
    
    
    agg = df.groupby('grupo').agg({'grupo':['count'], 'converteu':['mean']
                               , 'spending':['mean', 'sum']
                               , 'transacoes':['mean', 'sum']})
    print(agg)
    print("_____________________")
    
    #lift do resultado das tabelas acima

    print("Lift da conversão: "     + str(round(((agg.iloc[0,1] / agg.iloc[1,1]) -1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['converteu'], df.query('grupo == "controle"')['converteu'])
    print("Estatística t do teste de diferença de proporções: "+str(round(t,2)))
    print("P-valor do teste de diferença de proporções: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de conversão não é significativa")
    else:
        print("A diferença de conversão é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift do spending: "     + str(round(((agg.iloc[0,2] / agg.iloc[1,2]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['spending_corr'], df.query('grupo == "controle"')['spending_corr'])
    print("Estatística t do teste de spending: "+str(round(t,2)))
    print("P-valor do teste de spending: "+str(round(p,6)))
    print("Conclusão:")
    if p >0.05:
        print("A diferença de spending não é significativa")
    else:
        print("A diferença de spending é estatisticamente significativa")
    print("______________________________________________________________")
    
    print("Lift da frequência: "     + str(round(((agg.iloc[0,4] / agg.iloc[1,4]) - 1)  * 100,2)) + str('%'))
    from scipy import stats
    t, p = stats.ttest_ind(df.query('grupo == "alvo"')['transacoes_corr'], df.query('grupo == "controle"')['transacoes_corr'])
    print("Estatística t do teste de frequencia: "+str(round(t,2)))
    print("P-valor do teste de frequencia: "+str(round(p,6)))
    if p >0.05:
        print("A diferença de frequência não é significativa")
    else:
        print("A diferença de frequência é estatisticamente significativa")
    print("______________________________________________________________")
    


def report_perfil_grupos(df2, coluna_grupo, coluna_contagem, dif_var, n_var, colunas_perfil):

    from collections import OrderedDict
    
    casos_signif_final = pd.DataFrame([], columns=['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs'])
    casos_signif_final[coluna_grupo] = []

    
    chave = colunas_perfil
    chave.append(coluna_grupo)
    chave.append(coluna_contagem)

    kpis = ['n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca',	'diferenca_abs']
    
    df = df2[chave]
    df[coluna_contagem] = df[coluna_contagem].astype('int')

    for i in list(range(2, len(colunas_perfil)-1)):
        
        #agregação por combinações
        col_agg1 = df.iloc[:,:i].columns.to_list()
        agg1 = df.groupby(col_agg1, as_index=False).agg({coluna_contagem:['sum']})       
        agg1.columns = agg1.columns.droplevel(0)
        agg1.reset_index()
        
        col_agg1.append('n_seg')
        agg1.columns = col_agg1
        col_agg1 = df.iloc[:,:i].columns.to_list() 
        
        
        #agreagação por perfil e grupo
        col_agg2 = df.iloc[:,:i].columns.to_list()
        col_agg2.append(coluna_grupo) #não estou conseguindo juntar perfil e grupo
        agg2 = df.groupby(col_agg2, as_index=False).agg({coluna_contagem:['sum']})
        
        agg2.columns = agg2.columns.droplevel(0)
        agg2.reset_index()
        col_agg2.append('n_seg_grupo')
        agg2.columns = col_agg2
        col_agg2 = df.iloc[:,:i].columns.to_list()              
        
        # agregação por consolidado grupo
        agg3= df.groupby(coluna_grupo).agg({coluna_contagem:['sum']})
        agg3.columns = agg3.columns.droplevel(0)
        agg3 = agg3.reset_index()
        agg3 = agg3.rename(columns = {'sum':'n_grupo'})
        
        
        # levar agg1 para agg2
        dfr2 = agg2.join(agg1.set_index(col_agg1), on = col_agg1, how = 'left')
        dfr2 = dfr2.join(agg3.set_index(coluna_grupo), on = coluna_grupo, how = 'left')
        total = agg3['n_grupo'].sum()
        # criar percentuais
        dfr2['total'] = total
        dfr2['per_grupo_seg'] = dfr2.n_seg_grupo / dfr2.n_seg
        dfr2['per_grupo_total'] = dfr2.n_grupo / dfr2.total
        dfr2['diferenca'] = dfr2['per_grupo_seg'] - dfr2['per_grupo_total'] 
        dfr2['diferenca_abs'] = np.abs(dfr2['diferenca']) 
        casos_signif = dfr2[dfr2['diferenca_abs'] > dif_var]
        casos_signif = casos_signif[casos_signif['n_seg_grupo'] > n_var]    
        casos_signif['segmento'] = ' '
        for i in casos_signif.drop(coluna_grupo, axis = 1).drop(kpis, axis = 1).columns:
            casos_signif[i].fillna("", inplace = True)
            casos_signif['segmento'] = casos_signif['segmento'] + ' - ' + casos_signif[i].astype("string")
        casos_signif = casos_signif[['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs']].join(casos_signif[[coluna_grupo]])
        casos_signif['segmento'] = (casos_signif['segmento'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        casos_signif_final = casos_signif_final.append(casos_signif).sort_values('diferenca_abs', ascending = False)
        
    colunas_perfil.remove(coluna_grupo)
    colunas_perfil.remove(coluna_contagem)
    

    for i in colunas_perfil:
        
        col_agg4 = df[[i]].columns.to_list()
        agg4 = df.groupby(col_agg4, as_index=False).agg({coluna_contagem:['sum']})       
        agg4.columns = agg4.columns.droplevel(0)
        agg4.reset_index()
        
        col_agg4.append('n_seg')
        agg4.columns = col_agg4
        col_agg4 = df[[i]].columns.to_list()
        
               
        #agreagação por perfil e grupo
        col_agg5 = df[[i]].columns.to_list()
        col_agg5.append(coluna_grupo)
        agg5 = df.groupby(col_agg5, as_index=False).agg({coluna_contagem:['sum']})
        agg5.columns = agg5.columns.droplevel(0)
        agg5.reset_index()
        col_agg5.append('n_seg_grupo')
        agg5.columns = col_agg5
        col_agg5 = df[[i]].columns.to_list()            

        # agregação por consolidado grupo
        agg6= df.groupby(coluna_grupo).agg({coluna_contagem:['sum']})
        agg6.columns = agg6.columns.droplevel(0)
        agg6 = agg6.reset_index()
        agg6 = agg6.rename(columns = {'sum':'n_grupo'})
                
        # levar agg1 para agg2
        dfr4 = agg5.join(agg4.set_index(col_agg4), on = col_agg4, how = 'left')
        dfr4 = dfr4.join(agg6.set_index(coluna_grupo), on = coluna_grupo, how = 'left')
        total = agg6['n_grupo'].sum()
        # criar percentuais
        dfr4['total'] = total
        dfr4['per_grupo_seg'] = dfr4.n_seg_grupo / dfr4.n_seg
        dfr4['per_grupo_total'] = dfr4.n_grupo / dfr4.total
        dfr4['diferenca'] = dfr4['per_grupo_seg'] - dfr4['per_grupo_total'] 
        dfr4['diferenca_abs'] = np.abs(dfr4['diferenca']) 
        casos_signif = dfr4[dfr4['diferenca_abs'] > dif_var]
        casos_signif = casos_signif[casos_signif['n_seg_grupo'] > n_var]    
        casos_signif['segmento'] = ' '
        
        for i in casos_signif.drop(coluna_grupo, axis = 1).drop(kpis, axis = 1).columns:
            casos_signif[i].fillna("", inplace = True)
            casos_signif['segmento'] = casos_signif['segmento'] + ' - ' + casos_signif[i].astype("string")
        casos_signif = casos_signif[['segmento', 'n_seg_grupo', 'n_seg','n_grupo','total','per_grupo_seg','per_grupo_total','diferenca','diferenca_abs']].join(casos_signif[[coluna_grupo]])
        casos_signif['segmento'] = (casos_signif['segmento'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        casos_signif_final = casos_signif_final.append(casos_signif).sort_values('diferenca_abs', ascending = False)        
        
    
    return casos_signif_final
    print('report dos perfis finalizado!')
    casos_signif_final.to_csv('report_perfil_total.csv') 


 
