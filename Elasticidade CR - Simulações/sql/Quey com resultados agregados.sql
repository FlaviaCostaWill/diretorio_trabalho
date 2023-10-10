select * from public.rentabilidade_cartoes_diego_camilo
limit 10


select * from cx_sandbox_zone.report_perdas_cr
where clientes_recorte_total > clientes_cx 
limit 5


select
mesref
, sum(ltv) as montante_ltv
, avg(ltv) as media_ltv
, approx_percentile(ltv, 0.5) as mediana_ltv
,  count(distinct l.cpf) as clientes_rentaveis_total
from public.rentabilidade_cartoes_diego_camilo l
left join growth_curated_zone.proposal_analysis pr on (pr.cpf = l.cpf)
where date_diff('month', cast(pr.dt_cfi_account as date), current_date) <= 12
group by 1
order by mesref




----------------------------------------------------------
--------- DADOS PARA PERDA DE ESTIMATIVA DE CR -----------
----------------------------------------------------------
with parametro as (
---------------- Queda de 10% de CR ---------------------
select 0.10 as queda_cr
)
, calculo_clientes as (
select 
	---- Clientes que deixam de ser atendidos  ------
round(clientes_cx * p.queda_cr, 0) as deixam_atendimento, 
round(clientes_cx * p.queda_cr  * perc_recorte, 0) as deixam_atendimento_recorte , 
round(clientes_cx * p.queda_cr  * perc_recorte * perc_churn_recorte * perc_recuperacao_churn, 0) as clientes_potencial_recuperacao
, *
from cx_sandbox_zone.report_perdas_cr
left join parametro as p on (1=1)
)
, calculo_ltv as (
	select *,
	--ltv por cliente  * clientes recuperados final (usando parametro de queda de cr)
	case when clientes_recorte_churn_recuperados > 0 then (LTV_Receita_Recuperado / clientes_recorte_churn_recuperados) * clientes_potencial_recuperacao else 0 end as ltv_receita_recuperado_final
	from calculo_clientes
)
, base_final as (
	select 
	dt_mes
	, hierarquia_1 
	, hierarquia_4
	, queda_cr
	, deixam_atendimento
	, perc_recorte
	, perc_churn_recorte
	, perc_recuperacao_churn
	, clientes_potencial_recuperacao
	, ltv_receita_recuperado_final
	-- Taxa geral de LTV por cliente que deixa de ser atendido
	, case when deixam_atendimento > 0 then ltv_receita_recuperado_final / deixam_atendimento else 0 end as ltv_incremental_cliente
	, case when media_ltv > 0 and deixam_atendimento > 0  then ltv_receita_recuperado_final / deixam_atendimento / media_ltv else 0 end as perc_ltv_cliente
	, case when montante_ltv  > 0 then ltv_receita_recuperado_final / montante_ltv else 0 end as perc_ltv_total
	from calculo_ltv
)
--select * from base_final --aberto por mes e hierarquia 4
select
dt_mes,
--incluir novas chaves de agregação
 sum(ltv_receita_recuperado_final) as ltv_receita_recuperado_final
, sum(ltv_incremental_cliente * deixam_atendimento) / sum(deixam_atendimento) as ltv_incremental_cliente
, sum(perc_ltv_cliente * deixam_atendimento) / sum(deixam_atendimento) as perc_ltv_cliente
, sum(perc_ltv_total) as perc_ltv_total
from base_final
group by 1






select
* from martech_curated_zone.historico_acionamento_pmm_cliente
limit 3

select * from 
cx_analytics_zone.clientes_ativos_cxm
limit 5




select
mesref
, sum(ltv) as montante_ltv
, avg(ltv) as media_ltv
, approx_percentile(ltv, 0.5) as mediana_ltv
,  count(distinct cpf) as clientes_rentaveis_total
from public.rentabilidade_cartoes_diego_camilo
where ltv > 0
group by 1
order by mesref


select COUNT(distinct CPF) 
from public.rentabilidade_cartoes_diego_camilo 
where mesref = '2023-08'
limit 5




select * from customer_curated_zone.ca_dict_occupations  limit 3

					

WITH chats AS (
		SELECT cpf_customer
		, count(DISTINCT id_issue) qtd_chats 
		FROM cx_curated_zone.helpshift_issues  
                 WHERE dt_criacao_chat >= to_date('2022-07-01','yyyy-mm-dd') AND dt_criacao_chat < to_date('2023-07-01','yyyy-mm-dd')
                 GROUP BY 1
                 )
	, chats_recorte_detalhado AS (
		SELECT date_trunc('month', cast(c.dt_chat as date)) as dt_mes
                          , c.cpf_customer
                          , max(case when tipo_de_tag = 'Problema' then 1 else 0 end) as flag_problema
                          --finalizado por automacao
                          , max(nr_tpr) as nr_tpr
                          , max(nr_tma) as nr_tma
                          , max(nr_tmat) as nr_tmat
                          , max(nm_tag_hierarquia_1) as hierarquia_1
                          , max(nm_tag_hierarquia_4) as hierarquia_4
                     FROM cx_curated_zone.cx_contact_rate c
                LEFT JOIN cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
                LEFT JOIN cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
                LEFT JOIN processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
                LEFT JOIN chats AS cht ON cht.cpf_customer = c.cpf_customer 		
                    WHERE dt_chat >= to_date('2023-01-01','yyyy-mm-dd') and dt_chat < to_date('2023-07-01','yyyy-mm-dd')
                      AND c.id_issue is not null and nm_tag_hierarquia_1 <> 'Crédito e Cobrança'
                      AND issues_macro.ds_entidade_resolucao = 'agente'
                      --RECORTE
                      and cht.cpf_customer is not NULL AND cht.qtd_chats < 4 and issues_macro.nr_tmat > 90
                 GROUP BY 1,2
                 )
		, de_para AS (
			SELECT DISTINCT dt_mes 
                              , date_add('day', -91, dt_mes) as periodo_antes_inicio
                              , date_add('day', -1, dt_mes) as periodo_antes_fim
                              , date_add('month', 1, dt_mes) as periodo_depois_inicio
                              , date_add('month', 3, dt_mes) as periodo_depois_fim
                              , date_add('month', 2, dt_mes) as periodo_depois30_fim
                   FROM chats_recorte_detalhado
                         )
		, spending_antes_total_ AS (
						SELECT distinct s.cpf
                                  , s.id_autorizacao
                                  , c.dt_mes
                                  , periodo_antes_inicio
                                  , periodo_antes_fim
                                  , periodo_depois_inicio
                                  , periodo_depois_fim
                                  , periodo_depois30_fim
                                  , s.vl_transacao as vl_compra_antes_90d
                                  --, count(distinct s.id_autorizacao) as qt_compra_antes_90d
                                  , bc.dt_criacao_conta as dt_criacao_conta
                                  , s.dt_dia as dt_last_use_credit
                                  , a.dt_first_use_credit as dt_first_use_credit
                                  , gc.vl_current_limit as vl_current_limit
                             FROM platform_curated_zone.transaction_dedicada_spending s 
                       INNER JOIN de_para c
                               ON (cast(s.dt_dia as date) >= c.periodo_antes_inicio 
                              AND cast(s.dt_dia as date) <= c.periodo_antes_fim)--IGUAL OU MAIOR E IGUAL
                        LEFT JOIN (SELECT cpf
                                          , id_customer  
                                          , min(cd_yearmonth) as dt_criacao_conta
                             FROM customer_curated_zone.ca_book_cliente
                         GROUP BY 1,2) bc 
                               ON (bc.cpf = s.cpf)
                        LEFT JOIN customer_curated_zone.ca_book_status_cliente a 
                               ON (a.id_customer = bc.id_customer)--limite
                        LEFT JOIN customer_curated_zone.ca_book_gestao_carteira gc 
                               ON (s.cpf = gc.cpf 
                              AND cast(replace(substring(cast(c.dt_mes as varchar),1,7),'-','') as double)  = gc.cd_yearmonth)
                            WHERE s.vl_transacao > 0 
                              AND bc.dt_criacao_conta IS NOT NULL
                              AND gc.vl_current_limit IS NOT NULL)
                         , spending_antes_total AS (
                         select cpf
                                ,dt_mes  
                                ,periodo_antes_inicio
                                ,periodo_antes_fim
                                ,periodo_depois_fim
                                ,periodo_depois30_fim
                                ,periodo_depois_inicio
                                ,dt_criacao_conta
                                ,sum(vl_compra_antes_90d) as vl_compra_antes_90d
                                ,count(distinct id_autorizacao) as qt_compra_antes_90d
                                ,max(dt_last_use_credit) as dt_last_use_credit
                                ,min(dt_first_use_credit) as dt_first_use_credit
                                ,max(vl_current_limit) as vl_current_limit
                         from spending_antes_total_
                         group by 1,2,3,4,5,6,7,8)
            	, base_total_add_chats AS (
            			SELECT s.cpf
                                  , s.dt_mes as dt_mes_true
                                  , s.periodo_antes_inicio
                                  , s.periodo_antes_fim
                                  , s.periodo_depois_inicio
                                  , s.periodo_depois_fim
                                  , s.periodo_depois30_fim
                                  , s.vl_compra_antes_90d
                                  , s.qt_compra_antes_90d
                                  , s.dt_last_use_credit
                                  , s.dt_first_use_credit
                                  , s.dt_criacao_conta
                                  , s.vl_current_limit
                                  , c.*
                                  , case when c.cpf_customer is null then 0 else 1 end as usou_chat
                             FROM chats_recorte_detalhado c
                        LEFT JOIN spending_antes_total s on (s.cpf = c.cpf_customer and c.dt_mes = s.dt_mes)
                        )
         	, spending_depois_30 as (select c.cpf
                                    , c.dt_mes_true
                                    , sum(s_30.vl_transacao) as vl_compra_depois_30d
                                    , count(distinct s_30.id_autorizacao) as qt_compras_depois_30d
                                    from base_total_add_chats c    
                            left join platform_curated_zone.transaction_dedicada_spending s_30
                                     on (c.cpf = s_30.cpf and
                                     cast(s_30.dt_dia as date) >= c.periodo_depois_inicio and
                                     cast(s_30.dt_dia as date) <= c.periodo_depois30_fim)
                                group by 1,2)
			, spending_depois_60 as (
         			select c.cpf
                                        , c.dt_mes_true
                                        , sum(s_60.vl_transacao) as vl_compra_depois_60d
                                        , count(distinct s_60.id_autorizacao) as qt_compras_depois_60d
                                    from base_total_add_chats c    
                                left join platform_curated_zone.transaction_dedicada_spending s_60
                                      on (c.cpf = s_60.cpf and
                                    cast(s_60.dt_dia as date) >= c.periodo_depois_inicio and
                                    cast(s_60.dt_dia as date) <= c.periodo_depois_fim)
                                group by 1,2
                                )
 				,  union_ltv as (
					select distinct 
					mesref
					, tipo
					, dias_atraso 
					, cpf
					, spending_credito
					, rec_itr
					, rec_recarga
					, rec_demais_encargos
					, desp_pdd
					, rec_juros
					, custos
					, ltv
					from public.rentabilidade_cartoes_diego_camilo 
					)
					, base_final as (
                    SELECT  a.cpf
                                , a.dt_mes_true
                                , a.periodo_antes_inicio
                                , a.periodo_antes_fim
                                , a.periodo_depois_inicio
                                , a.periodo_depois_fim
                                , a.periodo_depois30_fim
                                , a.qt_compra_antes_90d
                                , a.dt_last_use_credit
                                , a.vl_current_limit
                                , a.flag_problema
                                , a.nr_tpr
                                , a.nr_tma
                                , a.nr_tmat
                                , a.usou_chat
                                , a.hierarquia_1
                                , a.hierarquia_4
                                --checar se precisa
                                , gro_.ds_gender
                                , gro_.ds_state_abbreviation
                                , gro_.dt_birth
                                , gro_.ds_schooling
                                , gro_.ds_marital_status
                                , gro_.dt_opening_account
                                , gro_.ds_city_size
                                --fim
                                , max(a.vl_compra_antes_90d) vl_compra_antes_90d
                                , min(a.dt_first_use_credit) dt_first_use_credit
                                , max(gro_.vl_renda_declarada) vl_renda_declarada
                                , min(a.dt_criacao_conta) dt_criacao_conta
                                , count(distinct substring(cast(dt_event as varchar), 1, 10)) as dias_uso_app
                                , max(d1.vl_compra_depois_30d) vl_compra_depois_30d
                                , max(d1.qt_compras_depois_30d) qt_compras_depois_30d
                                , max(d2.vl_compra_depois_60d) vl_compra_depois_60d
                                , max(d2.qt_compras_depois_60d) qt_compras_depois_60d
                                , max(l.spending_credito) as spending_ltv
                                , max(l.rec_itr) as rec_itr
                                , max(l.rec_recarga) as rec_recarga
								, max(l.rec_demais_encargos) as rec_demais_encargos
								, max(l.desp_pdd) as desp_pdd
								, max(l.rec_juros) as rec_juros
								, max(l.custos) as custos
                                , max(l.ltv) as ltv
                                , max(l.dias_atraso) as dias_atraso_ltv
                           FROM base_total_add_chats a
                      LEFT JOIN customer_curated_zone.ca_book_cliente b_c ON (a.cpf = b_c.cpf)
                      LEFT JOIN customer_curated_zone.ca_book_growth gro_ ON gro_.id_customer = b_c.id_customer 
                      LEFT JOIN customer_curated_zone.ca_analitico_geoloc ap on (
                                  ap.cpf = a.cpf and
                                  ap.dt_event >= a.periodo_antes_inicio and
                                  ap.dt_event <= a.periodo_antes_fim)
                      LEFT JOIN spending_depois_30 d1 on (a.cpf = d1.cpf and a.dt_mes_true = d1.dt_mes_true)
                      LEFT JOIN spending_depois_60 d2 on (a.cpf = d2.cpf and a.dt_mes_true = d2.dt_mes_true)
                      left join union_ltv l on (a.cpf = l.cpf and a.dt_mes_true = cast(concat(l.mesref, '-01') as date))
                      where a.usou_chat = 1
                      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
                      )
                      --, contagem_clientes_chat_total_recorte as ()
                      --, contagem_clientes_churn as () --total e recorte
                      --, clientes_churn_scorados_modelo --simulado
                      --, contagem_clientes_perdendo_propensao --alvo da simulacao
                      --, spending e LTV médio dos clientes sem churn
                      --, 
                      select * from base_final
                      limit 5
                      
                      
                      
                      
                      