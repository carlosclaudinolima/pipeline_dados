import json as jn
import csv

def leitura_json(path_json):
    lista = []
    with open(path_json) as arquivo_json:
        lista = jn.load(arquivo_json)    
    return lista

def leitura_csv(path_csv):
    lista_b = []
    with open(path_csv, 'r') as arquivo_csv:
        csv_b = csv.DictReader(arquivo_csv)  
        for linha in csv_b:
            lista_b.append(linha)
    return lista_b

def leitura_dados(path_arquivo, tipo_arquivo):
    lista = []
    if tipo_arquivo == 'csv':
        lista = leitura_csv(path_arquivo)
    elif tipo_arquivo == 'json':
        lista =  leitura_json(path_arquivo)
    return lista

def get_columns(dados):
    return list(dados[-1].keys())

def rename_columns(dados, key_mapping):
    new_dados_csv = []
    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(data):
    return len(data)

def join(lista_a, lista_b):
    combined_list = []
    combined_list.extend(lista_a)
    combined_list.extend(lista_b)
    return combined_list

def transformando_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]
    for row in dados:
        linha = []
        for col in nomes_colunas:
            linha.append(row.get(col, 'Indisponível'))
        dados_combinados_tabela.append(linha)
    return dados_combinados_tabela    

def salvando_dados(path_salvar, dados):
    with open(path_salvar, 'w') as arquivo_dados:
        writer = csv.writer(arquivo_dados)
        writer.writerows(dados)

url_empresa_a = 'data_raw/dados_empresaA.json'
url_empresa_b = 'data_raw/dados_empresaB.csv'

# Iniciando a leitura
dados_json = leitura_dados(url_empresa_a, 'json')
nome_colunas_json = get_columns(dados_json)
tamanho_dados_json = size_data(dados_json)
print(f'Nome das colunas do JSON : {nome_colunas_json}')
print(f'Tamanho dos dados JSON : {tamanho_dados_json}')


dados_csv = leitura_dados(url_empresa_b, 'csv')
tamanho_dados_csv = size_data(dados_csv)
nome_colunas_csv = get_columns(dados_csv)
print(f'Nome das colunas do CSV : {nome_colunas_csv}')
print(f'Tamanho dos dados CSV : {tamanho_dados_csv}')

# Transformação dos dados
key_mapping = {
    'Nome do Item':'Nome do Produto',
    'Classificação do Produto':'Categoria do Produto',
    'Valor em Reais (R$)':'Preço do Produto (R$)',
    'Quantidade em Estoque':'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda':'Data da Venda'
}

dados_csv = rename_columns(dados_csv, key_mapping)
novo_nome_colunas_csv = get_columns(dados_csv)
print(f'Novo nome das colunas do CSV : {novo_nome_colunas_csv}')

dados_fusao = join(dados_json, dados_csv)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)

print(f'Nome das colunas da Fusão : {nome_colunas_fusao}')
print(f'Tamanho dos dados JSONda Fusão : {tamanho_dados_fusao}')

# Salvando dados
dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)
path_dados_combinados = 'data_processed/dados_combinados.csv'
salvando_dados(path_dados_combinados, dados_fusao_tabela)
print(path_dados_combinados)

#teste