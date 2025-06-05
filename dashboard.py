import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from databricks import sql
import json
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv('config.env')

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Vendas - Databricks",
    page_icon="📊",
    layout="wide"
)

# CSS customizado (com melhorias para badges)
st.markdown("""
<style>
    body, .main, .block-container {
        background-color: #1a252f !important;
        color: white !important;
    }
    
    .stButton > button {
        background-color: #26303A;
        color: white;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    
    .stButton > button:hover {
        background-color: #3ba8d0;
        border-color: #3ba8d0;
    }
    
    .metric-card {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 1rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }
    
    .metric-title {
        font-size: 0.85rem;
        color: #a0aec0;
        margin-bottom: 0.25rem;
    }
    
    .metric-value {
        font-size: 1.3rem;
        font-weight: bold;
        color: white;
    }
    
    .status-badge {
        padding: 4px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: bold;
        text-align: center;
        display: inline-block;
    }
    
    .status-em-estoque {
        background-color: #10b981;
        color: white;
    }
    
    .status-sem-estoque {
        background-color: #ef4444;
        color: white;
    }
    
    .status-estoque-baixo {
        background-color: #f59e0b;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Configurações Databricks a partir de variáveis de ambiente
DATABRICKS_HOSTNAME = os.getenv('DATABRICKS_HOSTNAME')
HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')

# Verificar se as credenciais foram carregadas
if not all([DATABRICKS_HOSTNAME, HTTP_PATH, ACCESS_TOKEN]):
    st.error("""
    ❌ **Erro de Configuração**: Credenciais do Databricks não encontradas!
    
    Por favor, certifique-se de que o arquivo `config.env` existe e contém:
    - DATABRICKS_HOSTNAME
    - DATABRICKS_HTTP_PATH  
    - DATABRICKS_ACCESS_TOKEN
    """)
    st.stop()

@st.cache_resource
def get_databricks_connection():
    """Cria conexão com Databricks"""
    try:
        return sql.connect(
            server_hostname=DATABRICKS_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        )
    except Exception as e:
        st.error(f"Erro ao conectar: {e}")
        return None

# NOVA FUNÇÃO: Obter total de produtos únicos
@st.cache_data(ttl=300)
def get_total_products():
    """Obtém o total de produtos únicos nos dados"""
    connection = get_databricks_connection()
    if not connection:
        return 10
    
    try:
        query = """
        SELECT COUNT(DISTINCT TRIM(descricaoProduto)) as total_produtos
        FROM main.default.itens_venda_mm
        WHERE descricaoProduto IS NOT NULL
        AND LENGTH(TRIM(descricaoProduto)) > 0
        AND qtde > 0
        """
        
        df = pd.read_sql(query, connection)
        if not df.empty:
            return int(df.iloc[0]['total_produtos'])
        return 10
        
    except Exception as e:
        st.error(f"Erro ao obter total de produtos: {e}")
        return 10

# 1. PRODUTOS QUE MAIS SAEM (RANKING) - Modificada para usar parâmetro global
@st.cache_data(ttl=300)
def get_top_products(limit=15):
    """Ranking dos produtos mais vendidos"""
    connection = get_databricks_connection()
    if not connection:
        return pd.DataFrame()
    
    try:
        query = f"""
        SELECT 
            TRIM(descricaoProduto) as produto,
            CAST(SUM(qtde) as DOUBLE) as quantidade_total,
            CAST(COUNT(DISTINCT vendaId) as BIGINT) as num_vendas,
            CAST(SUM(valor) as DOUBLE) as valor_total,
            FIRST(un) as unidade
        FROM main.default.itens_venda_mm
        WHERE descricaoProduto IS NOT NULL
        AND LENGTH(TRIM(descricaoProduto)) > 0
        AND qtde > 0
        GROUP BY TRIM(descricaoProduto)
        ORDER BY quantidade_total DESC
        LIMIT {limit}
        """
        
        df = pd.read_sql(query, connection)
        
        # Garantir tipos corretos
        if not df.empty:
            df['quantidade_total'] = pd.to_numeric(df['quantidade_total'], errors='coerce')
            df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce')
            df = df.dropna(subset=['produto', 'quantidade_total'])
        
        return df
        
    except Exception as e:
        st.error(f"Erro na query: {e}")
        return pd.DataFrame()

# 2. SAZONALIDADE DOS PRODUTOS - Modificada para usar parâmetro global
@st.cache_data(ttl=300)
def get_seasonality_data(top_n=5):
    """Análise de sazonalidade mensal"""
    connection = get_databricks_connection()
    if not connection:
        return pd.DataFrame()
    
    try:
        # Usar top N produtos para análise de sazonalidade
        query = f"""
        WITH top_produtos AS (
            SELECT descricaoProduto
            FROM main.default.itens_venda_mm
            WHERE descricaoProduto IS NOT NULL
            GROUP BY descricaoProduto
            ORDER BY SUM(qtde) DESC
            LIMIT {top_n}
        )
        SELECT 
            descricaoProduto as produto,
            DATE_FORMAT(data, 'yyyy-MM') as mes,
            SUM(qtde) as quantidade
        FROM main.default.itens_venda_mm
        WHERE descricaoProduto IN (SELECT descricaoProduto FROM top_produtos)
        AND data IS NOT NULL
        GROUP BY descricaoProduto, DATE_FORMAT(data, 'yyyy-MM')
        ORDER BY mes, produto
        """
        
        df = pd.read_sql(query, connection)
        
        # Se não tiver campo data, criar sazonalidade simulada baseada no vendaId
        if df.empty:
            query_alt = f"""
            WITH top_produtos AS (
                SELECT descricaoProduto, COUNT(*) as cnt
                FROM main.default.itens_venda_mm
                WHERE descricaoProduto IS NOT NULL
                GROUP BY descricaoProduto
                ORDER BY cnt DESC
                LIMIT {top_n}
            )
            SELECT 
                iv.descricaoProduto as produto,
                iv.vendaId,
                iv.qtde as quantidade
            FROM main.default.itens_venda_mm iv
            INNER JOIN top_produtos tp ON iv.descricaoProduto = tp.descricaoProduto
            """
            
            df = pd.read_sql(query_alt, connection)
            if not df.empty:
                # Criar meses baseados no vendaId
                max_venda = df['vendaId'].max()
                df['mes_num'] = (df['vendaId'] / max_venda * 12).astype(int) % 12 + 1
                df['mes'] = pd.to_datetime(df['mes_num'], format='%m').dt.strftime('2024-%m')
                df = df.groupby(['produto', 'mes'])['quantidade'].sum().reset_index()
        
        return df
        
    except Exception as e:
        st.error(f"Erro: {e}")
        return pd.DataFrame()

# 3. ANÁLISE DE ESTOQUE - Modificada para incluir média por dia e usar parâmetro global
@st.cache_data(ttl=300)
def analyze_stock(top_n=10):
    """Verifica estoque dos produtos mais vendidos com média diária"""
    connection = get_databricks_connection()
    if not connection:
        return pd.DataFrame()
    
    try:
        query = f"""
        WITH vendas_resumo AS (
            SELECT 
                descricaoProduto,
                SUM(qtde) as total_vendido,
                AVG(qtde) as media_venda,
                COUNT(DISTINCT DATE(data)) as dias_com_venda,
                COUNT(DISTINCT vendaId) as num_vendas
            FROM main.default.itens_venda_mm
            WHERE descricaoProduto IS NOT NULL
            AND data IS NOT NULL
            GROUP BY descricaoProduto
            ORDER BY total_vendido DESC
            LIMIT {top_n}
        ),
        vendas_com_media_dia AS (
            SELECT *,
                CASE 
                    WHEN dias_com_venda > 0 THEN total_vendido / dias_com_venda
                    ELSE total_vendido / 30.0  -- fallback para 30 dias
                END as media_venda_dia
            FROM vendas_resumo
        )
        SELECT 
            v.descricaoProduto as produto,
            v.total_vendido,
            v.media_venda,
            v.media_venda_dia,
            v.num_vendas,
            CASE 
                WHEN p.id IS NOT NULL THEN 'Em Estoque' 
                ELSE 'Sem Estoque' 
            END as status_estoque,
            p.descricao as descricao_cadastro,
            p.estoque as quantidade_estoque,
            p.grupo,
            p.codigo_fab,
            CASE 
                WHEN p.id IS NOT NULL AND p.estoque IS NOT NULL AND v.media_venda_dia > 0 
                THEN p.estoque / v.media_venda_dia
                ELSE NULL
            END as dias_estoque
        FROM vendas_com_media_dia v
        LEFT JOIN main.default.produtos_mm p 
            ON LOWER(TRIM(v.descricaoProduto)) = LOWER(TRIM(p.descricao))
        ORDER BY v.total_vendido DESC
        """
        
        df = pd.read_sql(query, connection)
        return df
        
    except Exception as e:
        st.error(f"Erro: {e}")
        return pd.DataFrame()

# Header
st.markdown("""
<div style="text-align: left; margin-bottom: 2rem;">
    <h1 style="color: white; font-size: 2.5rem;">📊 Dashboard de Análise de Vendas</h1>
    <p style="color: #8b95a1;">Produtos mais vendidos, sazonalidade e análise de estoque</p>
</div>
""", unsafe_allow_html=True)

# CONTROLES GLOBAIS MELHORADOS
st.markdown("### ⚙️ Configurações Globais")

# Obter total de produtos para definir o range
total_produtos = get_total_products()

col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    # Slider para Top N
    top_n_slider = st.slider(
        "Número de produtos filtrados", 
        min_value=2, 
        max_value=min(total_produtos, 50),  # Limitar a 50 para performance
        value=min(10, total_produtos),
        key="top_n_slider"
    )

with col2:
    # Input numérico para facilitar digitação
    top_n_input = st.number_input(
        "Ou digite o número exato:", 
        min_value=2, 
        max_value=min(total_produtos, 100),
        value=top_n_slider,
        key="top_n_input"
    )

with col3:
    if st.button("🔄 Atualizar", key="refresh"):
        st.cache_data.clear()
        st.rerun()

# Usar o valor do input se diferente do slider, senão usar o slider
top_n = top_n_input if top_n_input != top_n_slider else top_n_slider

# Mostrar informação sobre o total
st.info(f"📊 Total de produtos únicos disponíveis: **{total_produtos:,}** | Filtrando top **{top_n}**")

st.markdown("---")

# Tabs principais
tab1, tab2, tab3 = st.tabs(["🏆 Ranking de Produtos", "📈 Sazonalidade", "📦 Análise de Estoque"])

with tab1:
    st.markdown(f"### Produtos que Mais Saem (Top {top_n} filtrados)")

    produtos_top = get_top_products(top_n)

    if produtos_top.empty:
        st.error("Nenhum dado retornado da query. Verifique a conexão com o banco.")
    else:
        # Garante que colunas opcionais existem
        for col in ['valor_total', 'num_vendas', 'unidade']:
            if col not in produtos_top.columns:
                produtos_top[col] = None

        # Limpa e converte campos obrigatórios
        produtos_top_clean = produtos_top.dropna(subset=['produto', 'quantidade_total'])
        produtos_top_clean['produto'] = produtos_top_clean['produto'].astype(str)
        produtos_top_clean['quantidade_total'] = pd.to_numeric(produtos_top_clean['quantidade_total'], errors='coerce')
        produtos_top_clean = produtos_top_clean.dropna(subset=['quantidade_total'])
        produtos_top_clean['quantidade_total'] = produtos_top_clean['quantidade_total'].astype(float)

        # Formata o campo valor_total para exibição (texto)
        produtos_top_clean['valor_total_formatado'] = produtos_top_clean['valor_total'].apply(
            lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(v) else "-"
        )

        # Converte para JSON seguro (sem numpy types)
        vega_data = produtos_top_clean.to_dict(orient='records')
        vega_data = [
            {
                k: float(v) if isinstance(v, (np.float64, np.int64)) else v
                for k, v in d.items()
            }
            for d in vega_data
        ]

        # Métricas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total de Produtos</div>
                <div class="metric-value">{len(produtos_top_clean)}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Quantidade Total Vendida</div>
                <div class="metric-value">{produtos_top_clean['quantidade_total'].sum():,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            primeiro_produto = produtos_top_clean.iloc[0]['produto'] if not produtos_top_clean.empty else "N/A"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Produto Líder</div>
                <div class="metric-value">{primeiro_produto[:20]}</div>
            </div>
            """, unsafe_allow_html=True)

        # Gráfico VegaLite
        vega_spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": vega_data},
            "mark": {"type": "bar", "cornerRadiusEnd": 4},
            "encoding": {
                "x": {
                    "field": "quantidade_total",
                    "type": "quantitative",
                    "title": "Quantidade Total Vendida"
                },
                "y": {
                    "field": "produto",
                    "type": "nominal",
                    "sort": "-x",
                    "title": "Produto"
                },
                "color": {
                    "field": "quantidade_total",
                    "type": "quantitative",
                    "scale": {"scheme": "blues"},
                    "legend": None
                },
                "tooltip": [
                    {"field": "produto", "title": "Produto"},
                    {"field": "quantidade_total", "title": "Quantidade", "format": ",.0f"},
                    {"field": "valor_total_formatado", "title": "Valor Total"},
                    {"field": "num_vendas", "title": "Nº de Vendas"},
                    {"field": "unidade", "title": "Unidade"}
                ]
            },
            "config": {
                "view": {"stroke": "transparent"},
                "axis": {
                    "labelColor": "#a0aec0",
                    "titleColor": "#ffffff",
                    "gridColor": "rgba(255,255,255,0.1)",
                    "domainColor": "rgba(255,255,255,0.2)"
                },
                "background": "transparent"
            },
            "width": "container",
            "height": 400
        }

        st.vega_lite_chart(vega_spec, use_container_width=True)

with tab2:
    st.markdown(f"### Análise de Sazonalidade ({top_n} produtos filtrados)")
    
    # Usar o número exato de produtos escolhidos pelo usuário
    sazonalidade = get_seasonality_data(top_n)
    
    if not sazonalidade.empty:
        # Gráfico de linhas Vega
        vega_spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": sazonalidade.to_dict('records')},
            "mark": {"type": "line", "point": True},
            "encoding": {
                "x": {
                    "field": "mes",
                    "type": "ordinal",
                    "title": "Mês"
                },
                "y": {
                    "field": "quantidade",
                    "type": "quantitative",
                    "title": "Quantidade Vendida"
                },
                "color": {
                    "field": "produto",
                    "type": "nominal",
                    "title": "Produto",
                    "scale": {"scheme": "category10"}
                },
                "tooltip": [
                    {"field": "produto", "title": "Produto"},
                    {"field": "mes", "title": "Mês"},
                    {"field": "quantidade", "title": "Quantidade", "format": ",.0f"}
                ]
            },
            "config": {
                "view": {"stroke": "transparent"},
                "axis": {
                    "labelColor": "#a0aec0",
                    "titleColor": "#fff",
                    "gridColor": "rgba(255,255,255,0.1)"
                },
                "legend": {
                    "labelColor": "#a0aec0",
                    "titleColor": "#fff"
                },
                "background": "transparent"
            },
            "width": "container",
            "height": 400
        }
        
        st.vega_lite_chart(vega_spec, use_container_width=True)
        
        # Insights de sazonalidade
        st.info(f"""
        💡 **Insights de Sazonalidade ({top_n} produtos filtrados):**
        - Produtos com alta variação mensal podem precisar de ajuste de estoque sazonal
        - Picos consistentes indicam oportunidades de promoção
        - Quedas bruscas podem indicar ruptura de estoque
        """)
    else:
        st.warning("Dados de sazonalidade não disponíveis ou insuficientes.")

with tab3:
    st.markdown(f"### Status de Estoque ({top_n} produtos filtrados)")
    
    estoque_df = analyze_stock(top_n)
    
    if not estoque_df.empty:
        # Adicionar badges de status
        def format_status_badge(status):
            if status == 'Em Estoque':
                return '<span class="status-badge status-em-estoque">✅ Em Estoque</span>'
            else:
                return '<span class="status-badge status-sem-estoque">❌ Sem Estoque</span>'
        
        # Criar cópia para exibição com badges
        display_df = estoque_df.copy()
        
        # Criar coluna de status formatada para o dataframe
        display_df['status_visual'] = display_df['status_estoque'].apply(
            lambda status: f"✅ {status}" if status == 'Em Estoque' else f"❌ {status}"
        )
        
        # Formatar colunas para melhor exibição
        if 'media_venda_dia' in display_df.columns:
            display_df['media_venda_dia'] = display_df['media_venda_dia'].round(2)
        if 'dias_estoque' in display_df.columns:
            display_df['dias_estoque'] = display_df['dias_estoque'].round(1)
        
        # Métricas de estoque
        sem_estoque = len(estoque_df[estoque_df['status_estoque'] == 'Sem Estoque'])
        com_estoque = len(estoque_df[estoque_df['status_estoque'] == 'Em Estoque'])
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">✅ Em Estoque</div>
                <div class="metric-value">{com_estoque}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">❌ Sem Estoque</div>
                <div class="metric-value">{sem_estoque}</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">📊 Taxa de Ruptura</div>
                <div class="metric-value">{(sem_estoque/len(estoque_df)*100):.1f}%</div>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            media_dia_total = display_df['media_venda_dia'].sum() if 'media_venda_dia' in display_df.columns else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">📈 Média Vendas/Dia</div>
                <div class="metric-value">{media_dia_total:.1f}</div>
            </div>
            """, unsafe_allow_html=True)
        

        st.markdown("---")
        
        # Também manter a tabela original do Streamlit para funcionalidades de filtro
        st.markdown("#### 🔍 Tabela Interativa (com filtros)")
        st.caption("💡 Use esta tabela para filtrar e ordenar os dados")
        
        # Função para destacar linhas baseado no status
        def highlight_status_rows(row):
            if row['status_estoque'] == 'Sem Estoque':
                return ['background-color: rgba(239, 68, 68, 0.15); color: white'] * len(row)
            elif row['status_estoque'] == 'Em Estoque':
                return ['background-color: rgba(16, 185, 129, 0.15); color: white'] * len(row)
            else:
                return [''] * len(row)
        
        # Preparar DataFrame para a tabela interativa (manter status_estoque para o estilo)
        display_df_for_table = display_df.copy()
        
        # Aplicar estilo condicional
        styled_df = display_df_for_table.style.apply(highlight_status_rows, axis=1)
        
        # Configurar colunas da tabela
        column_config_filtered = {
            "produto": st.column_config.TextColumn("Produto", help="Nome do produto"),
            "total_vendido": st.column_config.NumberColumn("Total Vendido", format="%d"),
            "media_venda": st.column_config.NumberColumn("Média/Venda", format="%.1f"),
            "media_venda_dia": st.column_config.NumberColumn("Média/Dia", format="%.2f", help="Média de vendas por dia"),
            "num_vendas": st.column_config.NumberColumn("Nº Vendas", format="%d"),
            "status_visual": st.column_config.TextColumn("Status", help="Status do produto no estoque"),
            "quantidade_estoque": st.column_config.NumberColumn("Qtd Estoque", format="%d"),
            "dias_estoque": st.column_config.NumberColumn("Dias Estoque", format="%.1f", help="Quantos dias de estoque restam"),
            "grupo": st.column_config.TextColumn("Grupo"),
            "codigo_fab": st.column_config.TextColumn("Cód. Fabricante")
        }
        
        # Exibir tabela interativa com filtros e destaque visual
        st.dataframe(
            styled_df,
            column_config=column_config_filtered,
            hide_index=True,
            use_container_width=True,
            height=400,  # Altura menor já que temos a tabela visual acima
            column_order=["produto", "total_vendido", "media_venda", "media_venda_dia", "num_vendas", "status_visual", "quantidade_estoque", "dias_estoque", "grupo", "codigo_fab"]
        )

         # Alertas detalhados
        produtos_criticos = estoque_df[estoque_df['status_estoque'] == 'Sem Estoque']['produto'].tolist()
        if produtos_criticos:
            produtos_html = "<br>".join([f"• {p}" for p in produtos_criticos[:5]])
            st.markdown(f"""
            <div style="background-color:#511; padding:1rem; border-radius:0.5rem; color:white">
                <strong>⚠️ PRODUTOS TOP VENDAS SEM CADASTRO/ESTOQUE:</strong><br><br>
                {produtos_html}<br><br>
                <em>Ação recomendada:</em> Verificar se estes produtos estão cadastrados corretamente no sistema.
            </div>
            <br>
            """, unsafe_allow_html=True)
        
        # Produtos com estoque baixo (menos de 7 dias)
        if 'dias_estoque' in estoque_df.columns:
            estoque_baixo = estoque_df[
                (estoque_df['status_estoque'] == 'Em Estoque') & 
                (estoque_df['dias_estoque'] < 7) &
                (estoque_df['dias_estoque'].notna())
            ]
            if not estoque_baixo.empty:
                produtos_html = "<br>".join([
                    f"• {row['produto']} - Estoque: {row['quantidade_estoque']:.0f} / Dias restantes: {row['dias_estoque']:.1f}"
                    for _, row in estoque_baixo.iterrows()
                ])

                # Renderiza como bloco visual semelhante ao st.warning
                st.markdown(f"""
                <div style="
                    background-color: #fdf6b2;
                    color: #856404;
                    border-left: 6px solid #ffeeba;
                    padding: 1rem;
                    border-radius: 0.5rem;
                    font-size: 0.95rem;
                ">
                    <strong>⚠️ PRODUTOS COM ESTOQUE BAIXO (menos de 7 dias de venda):</strong><br><br>
                    {produtos_html}
                </div>
                """, unsafe_allow_html=True)
        
        # Alertas detalhados
    else:
        st.error("Não foi possível carregar os dados de estoque.")

# Insights gerais
st.markdown("---")
st.markdown("### 💡 Recomendações Baseadas nos Dados")

col1, col2 = st.columns(2)
with col1:
    st.warning(f"""
    **📦 Gestão de Estoque ({top_n} produtos filtrados):**
    - Priorizar reposição dos produtos sem estoque no TOP {top_n}
    - Criar alertas para produtos próximos da ruptura
    - Ajustar níveis de estoque baseado na sazonalidade
    - Monitorar produtos com menos de 7 dias de estoque
    """)

with col2:
    st.success(f"""
    **📈 Oportunidades ({top_n} produtos filtrados):**
    - Focar promoções nos produtos de alta rotação
    - Preparar estoque extra para períodos de pico
    - Negociar melhores condições com fornecedores dos TOP produtos
    - Implementar reposição automática baseada na média diária
    """)

# Footer
st.markdown(f"""
<div style="margin-top: 3rem; text-align: center; color: #8b95a1; font-size: 0.9rem;">
   Dashboard de Vendas | Análise em Tempo Real com Databricks | {top_n} produtos filtrados de {total_produtos:,} totais
</div>
""", unsafe_allow_html=True)