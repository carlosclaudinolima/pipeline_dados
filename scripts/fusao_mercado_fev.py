from processamento_dados import Dados

url_empresa_a = 'data_raw/dados_empresaA.json'
url_empresa_b = 'data_raw/dados_empresaB.csv'

#Extract
dados_empresaA = Dados(url_empresa_a, 'json')
print(f'Nome das colunas da empresa A : {dados_empresaA.nomes_colunas}')
print(f'Tamanho dos dados empresa A : {dados_empresaA.qtd_linhas}')

dados_empresaB = Dados(url_empresa_b, 'csv')
print(f'Nome das colunas da empresa B : {dados_empresaB.nomes_colunas}')
print(f'Tamanho dos dados empresa B : {dados_empresaB.qtd_linhas}')
print('-'*40)

# Transform
key_mapping = {
    'Nome do Item':'Nome do Produto',
    'Classificação do Produto':'Categoria do Produto',
    'Valor em Reais (R$)':'Preço do Produto (R$)',
    'Quantidade em Estoque':'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda':'Data da Venda'
}

print('Renomeando clounas da empresa B...')
dados_empresaB.rename_columns(key_mapping)
print(f'Novos nomes das colunas da empresa B : {dados_empresaB.nomes_colunas}')
print(f'Tamanho dos dados empresa B : {dados_empresaB.qtd_linhas}')
print('-'*40)

print('Fusão dos dados...')
dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f'Tamanho dos dados da fusão : {dados_fusao.qtd_linhas}')

# Load
path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(f'\nDados da fusão salvos em  {path_dados_combinados}\n')