WITH chats AS (SELECT cpf_customer, count(DISTINCT id_issue) qtd_chats FROM cx_curated_zone.helpshift_issues  
                 WHERE dt_criacao_chat >= to_date('2021-01-01','yyyy-mm-dd') AND dt_criacao_chat < to_date('2022-01-01','yyyy-mm-dd')
                 GROUP BY 1)
		, chats_agg AS (
			SELECT date_trunc('month', cast(c.dt_chat as date)) as dt_mes
                          , c.cpf_customer
                          -- problema?
                          , max(case when tipo_de_tag = 'Problema' then 1 else 0 end) as flag_problema
                          --finalizado por automacao
                          , max(nr_tpr) as nr_tpr
                          , max(nr_tma) as nr_tma
                          , max(nr_tmat) as nr_tmat
                          , max(nm_tag_hierarquia_1) as hierarquia_1
                     FROM cx_curated_zone.cx_contact_rate c
                LEFT JOIN cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
                LEFT JOIN cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
                LEFT JOIN processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
               INNER JOIN chats AS cht ON cht.cpf_customer = c.cpf_customer 
               						AND cht.qtd_chats < 4 
                    WHERE dt_chat >= to_date('2022-01-01','yyyy-mm-dd') and dt_chat < to_date('2022-04-01','yyyy-mm-dd')
                      AND c.id_issue is not null and issues_macro.nr_tmat > 90 and nm_tag_hierarquia_1 <> 'Crédito e Cobrança'
                 GROUP BY 1,2
                 )
		, de_para AS (
		SELECT DISTINCT dt_mes 
                              , date_add('day', -91, dt_mes) as periodo_antes_inicio
                              , date_add('day', -1, dt_mes) as periodo_antes_fim
                              , date_add('month', 9, dt_mes) as periodo_depois_inicio
                              , date_add('month', 12, dt_mes) as periodo_depois_fim
                         FROM chats_agg
                         )
		, spending_antes_total_ AS (SELECT distinct s.cpf
                                  , s.id_autorizacao
                                  , c.dt_mes
                                  , periodo_antes_inicio
                                  , periodo_antes_fim
                                  , periodo_depois_inicio
                                  , periodo_depois_fim
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
                         GROUP BY 1,2)bc 
                               ON (bc.cpf = s.cpf)
                        LEFT JOIN customer_curated_zone.ca_book_status_cliente a 
                               ON (a.id_customer = bc.id_customer)--limite
                        LEFT JOIN customer_curated_zone.ca_book_gestao_carteira gc 
                               ON (s.cpf = gc.cpf 
                              AND cast(replace(substring(cast(c.dt_mes as varchar),1,7),'-','') as double)  = gc.cd_yearmonth)
                            WHERE s.vl_transacao > 0 
                              AND bc.dt_criacao_conta IS NOT NULL
                              AND gc.vl_current_limit IS NOT NULL)
                         , spending_antes_total AS (select cpf
                                ,dt_mes  
                                ,periodo_antes_inicio
                                ,periodo_antes_fim
                                ,periodo_depois_fim
                                ,periodo_depois_inicio
                                ,dt_criacao_conta
                                ,sum(vl_compra_antes_90d) as vl_compra_antes_90d
                                ,count(distinct id_autorizacao) as qt_compra_antes_90d
                                ,max(dt_last_use_credit) as dt_last_use_credit
                                ,min(dt_first_use_credit) as dt_first_use_credit
                                ,max(vl_current_limit) as vl_current_limit
                         from spending_antes_total_
                         group by 1,2,3,4,5,6,7)
            , base_total_add_chats AS (SELECT s.cpf
                                  , s.dt_mes as dt_mes_true
                                  , s.periodo_antes_inicio
                                  , s.periodo_antes_fim
                                  , s.periodo_depois_inicio
                                  , s.periodo_depois_fim
                                  , s.vl_compra_antes_90d
                                  , s.qt_compra_antes_90d
                                  , s.dt_last_use_credit
                                  , s.dt_first_use_credit
                                  , s.dt_criacao_conta
                                  , s.vl_current_limit
                                  , c.*
                                  , case when c.cpf_customer is null then 0 else 1 end as usou_chat
                             FROM spending_antes_total s
                        LEFT JOIN chats_agg c on (s.cpf = c.cpf_customer and c.dt_mes = s.dt_mes))
         	, spending_depois as (select c.cpf
                                        , c.dt_mes_true
                                        , sum(s_60.vl_transacao) as vl_compra_depois_ano
                                        , count(distinct s_60.id_autorizacao) as qt_compras_depois_ano
                                    from base_total_add_chats c    
                                left join platform_curated_zone.transaction_dedicada_spending s_60
                                      on (c.cpf = s_60.cpf and
                                    cast(s_60.dt_dia as date) >= c.periodo_depois_inicio and
                                    cast(s_60.dt_dia as date) <= c.periodo_depois_fim)
                                group by 1,2)
                    , base_completa as (
                                SELECT  a.cpf
                                , a.dt_mes_true
                                , a.qt_compra_antes_90d
                                , a.dt_last_use_credit
                                , a.vl_current_limit
                                , a.flag_problema
                                , a.nr_tpr
                                , a.nr_tma
                                , a.nr_tmat
                                , a.usou_chat
                                , a.hierarquia_1
                                , max(a.vl_compra_antes_90d) vl_compra_antes_90d
                                , min(a.dt_first_use_credit) dt_first_use_credit
                                , min(a.dt_criacao_conta) dt_criacao_conta
                                , count(distinct substring(cast(dt_event as varchar), 1, 10)) as dias_uso_app
                                , max(d2.vl_compra_depois_ano) vl_compra_depois_ano
                                , max(d2.qt_compras_depois_ano) qt_compras_depois_ano
                           FROM base_total_add_chats a
                      LEFT JOIN customer_curated_zone.ca_book_cliente b_c ON (a.cpf = b_c.cpf)
                      LEFT JOIN customer_curated_zone.ca_analitico_geoloc ap on (
                                  ap.cpf = a.cpf and
                                  ap.dt_event >= a.periodo_antes_inicio and
                                  ap.dt_event <= a.periodo_antes_fim)
                      LEFT JOIN spending_depois d2 on (a.cpf = d2.cpf and a.dt_mes_true = d2.dt_mes_true)
                      where a.usou_chat = 1
                      group by 1,2,3,4,5,6,7,8,9,10,11
                      )
                      select * from base_completa
                      
                      