import csv
import json as jn

class Dados:
    def __init__(self, path, tipo_dados):
        self.path = path
        self.tipo_dados = tipo_dados    
        self.dados = self.leitura_dados()
        self.nomes_colunas = self.get_columns()
        self.qtd_linhas = self.size_data()

    def leitura_json(self):
        lista = []
        with open(self.path) as arquivo_json:
            lista = jn.load(arquivo_json)    
        return lista

    def leitura_csv(self):
        lista_b = []
        with open(self.path, 'r') as arquivo_csv:
            csv_b = csv.DictReader(arquivo_csv)  
            for linha in csv_b:
                lista_b.append(linha)
        return lista_b

    def leitura_dados(self):
        lista = []
        if self.tipo_dados == 'csv':
            lista = self.leitura_csv()
        elif self.tipo_dados == 'json':
            lista =  self.leitura_json()
        elif self.tipo_dados == 'list':
            # Eu como programador a 30 anos achei isso horrível, mas foi ideia do professor
            lista = self.path
            self.path = 'lista em memoria'
        return lista
    
    def get_columns(self):
        return list(self.dados[-1].keys())
    
    def rename_columns(self, key_mapping):
        new_dados = []
        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        self.dados = new_dados
        # Atualizar as colunas
        self.nomes_colunas = self.get_columns()
        
    def size_data(self):
        return len(self.dados)
    
    def join(dadosA, dadosB):
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        return Dados(combined_list, 'list')
    
    def transformando_dados_tabela(self):
        dados_combinados_tabela = [self.nomes_colunas]
        for row in self.dados:
            linha = []
            for col in self.nomes_colunas:
                linha.append(row.get(col, 'Indisponível'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela    

    def salvando_dados(self, path_salvar):
        dados_combinados_tabela = self.transformando_dados_tabela()
        with open(path_salvar, 'w') as arquivo_dados:
            writer = csv.writer(arquivo_dados)
            writer.writerows(dados_combinados_tabela)


