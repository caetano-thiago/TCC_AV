from sqlalchemy import create_engine, text
from manipular_csv import Dados_csv
import pandas as pd

USER = 'meu_usuario'
PASSWORD = 'minha_senha'
DB = 'meu_banco'
HOST = 'localhost'
PORT = '5432'

engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')

def le_colunas(arquivo):
    data = Dados_csv(arquivo)
    return data.criar_df().columns

def coleta_dadosSensor(arquivo, amostras, coluna):
    dadosNormais = Dados_csv(arquivo)

    df = dadosNormais.criar_df()

    dadosNormais.fatiar_df(amostras)

    dadosNormais.eixo_df(coluna)

    dados = dadosNormais.series_df(coluna)

    return dados

colunas = le_colunas('12.csv')

series = {}

for coluna in colunas:
    series[coluna] = coleta_dadosSensor('12.csv', 50000, coluna)
    series[coluna].to_sql(coluna, engine, if_exists='replace', index=False)

print('Dados exportados com sucesso!')

def verificar_tabelas_normais(engine):
    with engine.connect() as conn:
        resultado = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'; "))
        tabelas = resultado.fetchall()
        print("Tabelas normais encontradas:")
        for tabela in tabelas:
            print(tabela[0])

verificar_tabelas_normais(engine)

