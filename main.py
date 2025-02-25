import pandas as pd
from sqlalchemy import create_engine, text
from scipy.stats import kurtosis
import numpy as np
from flask import Flask, jsonify, make_response, request


# Configuração da conexão com o banco de dados
USER = 'meu_usuario'
PASSWORD = 'minha_senha'
DB = 'meu_banco'
HOST = 'localhost'
PORT = '5432'

engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')


def verifica_hipertabelas():
    try: 
        with engine.connect() as con:
            resultado = con.execute(text('SELECT hypertable_name FROM timescaledb_information.hypertables;'))
            hipertabelas = resultado.fetchall()

            print('Hipertabelas encontradas:')
            for hipertabela in hipertabelas:
                print(hipertabela[0])
    except Exception as e:
        print(f'Não foi posível conectar ao banco de dados: {e}')

verifica_hipertabelas()

class Station:
    def __init__(self, id, descricao, status_treinamento=True):
        self.id = id 
        self.descricao = descricao
        self.status_treinamento = status_treinamento
        self.serie = None

    def medicao_station(self, tamanho_chunk=100000):
        query = f"SELECT * FROM {self.id};"
        # Inicializa uma lista para armazenar os chunks
        chunks = []
        for chunk in pd.read_sql(query, engine, chunksize=tamanho_chunk):
            chunks.append(chunk)
        # Concatena todos os chunks em um único DataFrame
        self.serie = pd.concat(chunks, ignore_index=True)
        return self.serie

    def muda_treinamento(self, novo_status):
        if self.status_treinamento == True:
            self.status_treinamento = novo_status
        else:
            self.status_treinamento = novo_status

    def calc_rms(self):          
        dados = np.array(self.serie[self.id])

        return np.sqrt(np.mean(dados**2))
    
    def calc_pico_pico(self):
        dados = np.array(self.serie[self.id])

        return np.ptp(dados)
    
    def calc_kurtosis(self):
        dados = np.array(self.serie[self.id])

        return kurtosis(dados)
    
    def calc_fator_crista(self):
        dados = np.array(self.serie[self.id])

        rms = np.sqrt(np.mean(dados**2))

        pico = np.max(np.abs(dados))

        return pico / rms

estacoes = {}

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route('/CriaEstacao', methods=['POST'])
def criar_estacao():
    estacao_nova = request.json # Recebe o JSON enviado de outro sistema
    id_estacao = estacao_nova.get("id")

    if not id_estacao or "descricao" not in estacao_nova:
        return make_response(jsonify(mensagem="Erro: Campos 'id' e 'descricao' são obrigatorio"),400)

    # Criando uma nova instância de Station e armazenando no dicionário
    estacoes[id_estacao] = Station(id=id_estacao, descricao=estacao_nova["descricao"])

    return make_response(jsonify(mensagem="Nova estacao iniciada.",
                                 estacoes_atuais=[{"id": key, "descricao": value.descricao} for key, value in estacoes.items()])
                                 ,201)

@app.route('/FazObservacao', methods=['POST'])
def fazObserva():
    observacao = request.json
    id_observacao = observacao.get("id")

    if not id_observacao or "tamanho_chunk" not in observacao:
        return make_response(jsonify(mensagem="Erro: Os campo 'id' e 'tamanho_chunk' não podem ser vazios"), 400)
    
    if id_observacao not in estacoes:
        return make_response(jsonify(mensagem="Erro: Estacao nao encontrada."), 404)
    
    estacao = estacoes[id_observacao]
    estacao.medicao_station(observacao["tamanho_chunk"])

    # Verifica se os dados foram carregados corretamente
    if estacao.serie is None or estacao.serie.empty:
        return make_response(jsonify(mensagem="Erro: Nenhum dado encontrado para essa estação."), 500)
    
    return make_response(jsonify(
        mensagem="Dados da estacao selecionada.",
        dados={
        "RMS": estacao.calc_rms(),
        "Pico a Pico": estacao.calc_pico_pico(),
        "Curtose": estacao.calc_kurtosis(),
        "Fator de Crista": estacao.calc_fator_crista()
    }
    ))



if __name__ == "__main__":
    app.run(debug=True)

