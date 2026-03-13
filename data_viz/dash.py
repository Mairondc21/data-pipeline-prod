import streamlit as st
import duckdb

st.set_page_config(
    page_title="Dashboard mystore.com",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)
def conection_duck_db():
    conn = duckdb.connect("lakehouse/gold_layer/gold_layer.duckdb", read_only=True)
    return conn

def main():
    session = conection_duck_db()

    #QUERYS
    tb_analise_estoque = session.sql(
        "SELECT * FROM main.gold_analise_estoque"
    ).df()

    tb_analise_avaliacoes = session.sql("""
        SELECT
            *
        FROM main.gold_analise_avaliacoes """
    ).df()

    tb_analise_rfm = session.sql(
        "SELECT * FROM main.gold_analise_rfm"
    ).df()

    #Filtros
    tb_analise_avaliacoes_tabela = tb_analise_avaliacoes[["nome_produto","avaliacao_media",
                                                          "qtd_avaliacoes_positivas",
                                                          "qtd_avaliacoes_negativas",
                                                          "qtd_avaliacoes_neutras"]]
    tb_analise_estoque_top_10 = tb_analise_estoque.sort_values(by=["quantidade_em_estoque"], ascending=False).head(20)
    tb_analise_rfm_score_5 = tb_analise_rfm[tb_analise_rfm["score_monetary"] == 5]

    #Graficos
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Quantidade de avaliações por produto")
        st.write(tb_analise_avaliacoes_tabela)
    with col2:
        st.markdown("### Top 20 maiores estoques por fornecedor")
        st.bar_chart(tb_analise_estoque_top_10,
                    x='fornecedor',
                    y='quantidade_em_estoque',
                    y_label="num_estoque",
                    color=["green"],
                    horizontal=True
        )
        
    st.markdown("### Perspecção de clientes")
    st.bar_chart(tb_analise_rfm_score_5,
                x='estado_envio',
                y='score_monetary',
                y_label="num_estoque",
                color=["green"],)


if __name__ == '__main__':
    main()