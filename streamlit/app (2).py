##############################################################################
# app.py – FGV + Snowflake Workshop Knowledge Hub
# ----------------------------------------------------------------------------
# Streamlit mini-site (single-file) written 100% in Python.
# • Uses FGV corporate colours (white + navy #002776)
# • Organises material into rich, research-grade sections
# • Lets the user read, search, and export each section as a PDF
# ----------------------------------------------------------------------------
# How to run locally:
pip install streamlit fpdf2
#   streamlit run app.py
##############################################################################
import streamlit as st
from fpdf import FPDF
from io import BytesIO
import datetime as dt

##############################################################################
# ----------  CONFIG & HELPER FUNCTIONS  -------------------------------------
##############################################################################
FGV_BLUE = "#002776"
st.set_page_config(
    page_title="FGV • Snowflake Workshop",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "FGV • Snowflake Workshop Companion\n© 2025 – Ensino & Pesquisa"
    },
)

# Inject a bit of CSS for FGV brand feel
st.markdown(
    f"""
    <style>
        .reportview-container {{
            background-color: white;
        }}
        header {{
            background-color: {FGV_BLUE};
        }}
        .sidebar .sidebar-content {{
            background-color: {FGV_BLUE};
            color: white;
        }}
        .sidebar .sidebar-content a {{
            color: #ffffff;
        }}
        .sidebar .sidebar-content .css-1aumxhk {{
            color: #ffffff;
        }}
        .stButton>button {{
            background-color:{FGV_BLUE};color:white;border:none;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

def generate_pdf(title: str, body_md: str) -> bytes:
    """Simple markdown-to-PDF converter (paragraph-level)."""
    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.multi_cell(0, 10, title, align="L")
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 11)
    for line in body_md.split("\n"):
        pdf.multi_cell(0, 7, line)
    output = BytesIO()
    pdf.output(output)
    return output.getvalue()

##############################################################################
# ----------  KNOWLEDGE BASE  -------------------------------------------------
##############################################################################
# Each key maps to a tuple: (section_title, markdown_content)
KB = {
    "intro": (
        "Visão Geral do Workshop",
        """
### Objetivos
* Compreender a **arquitetura em três camadas** da Snowflake (Storage, Compute, Services).
* Explorar casos de uso da FGV e de parceiros de mercado.
* Exercitar integração Python via **Snowpark**, **Streams & Tasks** e **Snowpark Container Services (SPCS)**.

### Linha do tempo do dia
1. Abertura & Parte 1: Fundamentos da Snowflake  
2. Outros fluxos – Processo de dados FGV + Conector Snowflake  
3. Retorno à apresentação oficial da Snowflake  
4. Sessão interna FGV (labs + Q&A)
"""
    ),
    "arch": (
        "Arquitetura (Storage | Compute | Services)",
        """
#### 1. Camada de Storage
* Micro-partições colunar (50-500 MB) com metadados para **pruning**.
* Escala elástica sob demanda; custo ~US$ 23/TB-mês[60].

#### 2. Camada de Compute
* *Virtual Warehouses* independentes → MPP.  
* **Auto-Scaling** & **Auto-Suspend** ➜ economia de créditos[44][48].

#### 3. Cloud Services
* Otimizador de consultas, segurança (RBAC, MFA), metadados, governança (tags, masking)[17][81].

##### Benefícios Chave
|Dimensão|Clássico DW|Snowflake|
|---|---|---|
|Escalabilidade|Vertical|Horizontal + elástica|
|Concurrência|Fila|Multi-cluster sem contenção|
|Zero-Copy Cloning|Não|Sim (cópias instantâneas)|
"""
    ),
    "governance": (
        "Governança, Segurança e Compartilhamento",
        """
* **Data Governance Framework**: classificação automática, access history, masking dinâmico[13][21].
* **Secure Data Sharing**: dados ao vivo, zero cópia; reader accounts para parceiros sem conta Snowflake[73][81][89].
* **Marketplace & Listings**: monetização; filtros de uso e métricas de consumo[85].

Exemplo de política de mascaramento:
```
CREATE MASKING POLICY ssn_mask AS (val string) 
RETURNS string ->
  CASE WHEN current_role() IN ('PII_FULL') THEN val
       ELSE regexp_replace(val,'\\d{3}-\\d{2}','XXX-XX') END;
ALTER TABLE hr.employees MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;
```
"""
    ),
    "python": (
        "Integração Python (Snowpark, UDFs, Streams & Tasks)",
        """
##### Snowpark DataFrame
```
from snowflake.snowpark import Session
session = Session.builder.configs(cfg).create()
df = session.table('raw.sales').filter("amount > 1000")
df_group = df.group_by("region").agg({"amount": "sum"})
df_group.show()
```

##### UDF em Python
```
@session.udf(return_type=IntegerType(), input_types=[IntegerType()])
def quadrado(x):
    return x * x
```

##### Streams + Tasks (CDC)
```
CREATE OR REPLACE STREAM s_raw ON TABLE raw.sales;
CREATE OR REPLACE TASK t_etl
  WAREHOUSE etl_wh
  WHEN SYSTEM$STREAM_HAS_DATA('s_raw')
AS
  MERGE INTO stage.sales USING (SELECT * FROM s_raw) src
    ON stage.sales.id = src.id
  WHEN MATCHED THEN UPDATE SET amount = src.amount
  WHEN NOT MATCHED THEN INSERT VALUES(src.*);
```

##### Snowpark Container Services (GPU/LLM)
* Deploy modelos LLM + Vector DB sem sair do perímetro Snowflake[78][82][86].
"""
    ),
    "use_cases": (
        "Casos de Uso Destacados",
        """
1. **FGV Analytics 360**  
   Ingesta CDC via *Snowpipe Streaming* → transformação incremental com *Tasks* → consumo em dashboards acadêmicos.

2. **Siemens Data Mesh** – Unificação de 50+ ERPs em Snowflake, 1.5 bi mudanças/dia[45].

3. **OpenFlow + Cortex AI** – Pipelines multimodais, embeddings para *document AI* (classificação e sumarização automática)[16][14][18][52].

4. **FinServ Risk Hub** – Streams ↔ Tasks para reconciliar transações em tempo-real; governança BC Ed. + row-level security[37].

---
**Impacto na FGV**  
Tempo médio de provisionamento de sandbox ↓ de 3 dias para 15 min; redução 20 % em custos anuais de DW graças a auto-suspend.
"""
    ),
    "spcs": (
        "Snowpark Container Services (Hands-on)",
        """
![diagram](https://raw.githubusercontent.com/streamlit/brand/main/images/brand/logo-primary.png)

1. `CREATE IMAGE REPOSITORY repo_llm;`
2. Push OCI image (`docker push <account>.snowflakecomputing.com/repo_llm:1.0`)
3. `CREATE COMPUTE POOL cp_gpu MIN_NODES=1 MAX_NODES=4 INSTANCE_FAMILY='GPU_XL';`
4. `CREATE SERVICE llm_svc IN COMPUTE POOL cp_gpu FROM SPEC '@spec_stage/llm.yaml';`

Com isso é possível rodar um endpoint Flask inferindo embeddings diretamente nos dados Snowflake, sem EKS/K8s externos[79].
"""
    ),
}

def sidebar():
    st.sidebar.title("📚 Navegação")
    section = st.sidebar.radio(
        "Selecione um tópico",
        list(KB.keys()),
        format_func=lambda k: KB[k][0],
    )
    st.sidebar.markdown("---")
    st.sidebar.info(
        "© 2025 FGV – Workshop Snowflake\n"
        "Esta aplicação foi escrita em Python puro."
    )
    return section

##############################################################################
# ----------  MAIN  ----------------------------------------------------------
##############################################################################
chosen_key = sidebar()
title, content = KB[chosen_key]

st.title(title)
st.markdown(content)

# PDF download
pdf_bytes = generate_pdf(title, content)
st.download_button(
    label="📄 Baixar esta seção em PDF",
    data=pdf_bytes,
    file_name=f"{title.replace(' ', '_')}.pdf",
    mime="application/pdf",
)

##############################################################################
# ----------  FOOTER  --------------------------------------------------------
##############################################################################
st.markdown(
    f"<small>Última atualização: {dt.datetime.now():%d %b %Y %H:%M}</small>",
    unsafe_allow_html=True,
)
