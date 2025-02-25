import pandas as pd

class Dados_csv:
    def __init__(self, nomearquivo):
        self.nomearquivo = nomearquivo
        self.dados = None
        self.fatia = None
        self.eixo = None

    def criar_df(self):
        coluna = []
        coluna = ['tacometro', 'x1', 'y1', 'z1', 'x2', 'y2', 'z2', 'micro']
        self.dados  = pd.read_csv(self.nomearquivo, sep=',', decimal='.', names=coluna)
        return self.dados

    def fatiar_df(self, indice):
        self.fatia = self.dados.iloc[:indice]
        return self.fatia
    
    def eixo_df(self, coluna):
        self.eixo = self.fatia[coluna]
        return self.eixo

    def series_df(self, coluna):
        if self.eixo is None:
            raise ValueError('Você precisa definir o eixo primeiro usando eixo_df()')


        time = pd.date_range(start='2025-02-22 00:00:00', periods=len(self.eixo),freq='20us')
        df = pd.DataFrame({'timestamp': time, coluna: self.eixo.values})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
       

